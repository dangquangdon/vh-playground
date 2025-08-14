from __future__ import annotations
import os
import traceback
import valohai

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import snowflake.ml.modeling.preprocessing as snowmlpp

from opentelemetry import trace
from snowflake.snowpark import Session
from snowflake import telemetry
from snowflake.ml.registry import registry
from snowflake.ml.modeling.pipeline import Pipeline
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.model_selection import GridSearchCV
from snowflake.ml.modeling.metrics import (
    mean_absolute_percentage_error,
    mean_squared_error,
)


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")


def get_login_token() -> str:
    try:
        with open("/snowflake/session/token", "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading token file: {e}")
        return None


def get_session_config() -> dict[str, str]:
    configs = {
        "account": SNOWFLAKE_ACCOUNT,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "host": SNOWFLAKE_HOST,
        "token": get_login_token(),
        "authenticator": "oauth",
        "role": SNOWFLAKE_ROLE,
    }

    return {k: v for k, v in configs.items() if v is not None}


def create_session(
    role: str | None = None,
    db_name: str | None = None,
    schema_name: str | None = None,
    warehouse_name: str | None = None,
) -> Session | None:
    config = get_session_config()
    print(f"Creating session with config: {config}")
    session = Session.builder.configs(config).create()
    if role and role != SNOWFLAKE_ROLE:
        session.use_role(role)

    if db_name and db_name != SNOWFLAKE_DATABASE:
        session.use_database(db_name)

    if schema_name and schema_name != SNOWFLAKE_SCHEMA:
        session.use_schema(schema_name)

    if warehouse_name and warehouse_name != SNOWFLAKE_WAREHOUSE:
        session.use_warehouse(warehouse_name)

    return session


# Before running, make sure @ML_PIPE_STAGE exists (SQL File)
# Create the sproc that creates and fits the pipeline based on the table passed
def train_save_ins_model(
    session: Session,
    source_of_truth: str,
    schema_name: str,
    major_version: bool = True,
) -> list[str]:
    log_messages = []
    tracer = trace.get_tracer("insurance_model.train")

    with tracer.start_as_current_span("train_save_ins_model"):
        try:
            telemetry.set_span_attribute("model.name", "INSURANCE_CHARGES_PREDICTION")

            # Data loading
            log_messages.append("Loading data from source of truth table")
            with tracer.start_as_current_span("data_loading"):
                # Access the data from the source of truth table
                df = session.table(source_of_truth).limit(10000)
                telemetry.set_span_attribute("data.row_count", df.count())
            log_messages.append("Data loaded successfully\n")

            # Feature engineering
            log_messages.append("Starting feature engineering")
            with tracer.start_as_current_span("feature_engineering"):
                # Define label and feature columns
                LABEL_COLUMNS = ["CHARGES"]
                FEATURE_COLUMN_NAMES = [
                    i for i in df.schema.names if i not in LABEL_COLUMNS
                ]
                log_messages.append(f"Feature columns: {FEATURE_COLUMN_NAMES}")
                OUTPUT_COLUMNS = ["PREDICTED_CHARGES"]

                # Define Snowflake numeric types (possibly for scaling, ordinal encoding)
                # numeric_types = [T.DecimalType, T.DoubleType, T.FloatType, T.IntegerType, T.LongType]
                # numeric_columns = [col.name for col in df.schema.fields if (type(col.datatype) in numeric_types) and (col.name in FEATURE_COLUMN_NAMES)]

                # Define Snowflake categorical types and determine which columns to OHE
                categorical_types = [T.StringType]
                cols_to_ohe = [
                    col.name
                    for col in df.schema.fields
                    if (type(col.datatype) in categorical_types)
                ]
                ohe_cols_output = [col + "_OHE" for col in cols_to_ohe]
                log_messages.append(f"Columns to OHE: {ohe_cols_output}")

                # Standardize the values in the rows by removing spaces, capitalizing
                def fix_values(columnn):
                    return F.upper(
                        F.regexp_replace(F.col(columnn), "[^a-zA-Z0-9]+", "_")
                    )

                for col in cols_to_ohe:
                    df = df.na.fill("NONE", subset=col)
                    df = df.withColumn(col, fix_values(col))
                telemetry.add_event("feature_engineering_complete")
            log_messages.append("Feature engineering completed\n")

            # Model training
            log_messages.append("Starting model training")
            with tracer.start_as_current_span("define_pipeline"):
                all_feature_cols = FEATURE_COLUMN_NAMES + ohe_cols_output
                # Define the pipeline
                log_messages.append(f"All feature columns: {all_feature_cols}")
                pipe = Pipeline(
                    steps=[
                        # ('imputer', SimpleImputer(input_cols=all_cols)),
                        # ('mms', snowmlpp.MinMaxScaler(input_cols=cols_to_scale, output_cols=scale_cols_output)),
                        (
                            "ohe",
                            snowmlpp.OneHotEncoder(
                                input_cols=cols_to_ohe,
                                output_cols=ohe_cols_output,
                                drop_input_cols=True,
                            ),
                        ),
                        (
                            "grid_search_reg",
                            GridSearchCV(
                                estimator=XGBRegressor(),
                                param_grid={
                                    "n_estimators": [50, 100, 200],  # 25
                                    "learning_rate": [0.01, 0.1, 0.5],  # .5
                                },
                                n_jobs=-1,
                                scoring="neg_mean_absolute_percentage_error",
                                input_cols=all_feature_cols,
                                label_cols=LABEL_COLUMNS,
                                output_cols=OUTPUT_COLUMNS,
                                drop_input_cols=True,
                            ),
                        ),
                    ]
                )
            log_messages.append("Pipeline defined\n")

            log_messages.append("Starting model training and evaluation")
            with tracer.start_as_current_span("train_test_split"):
                # Split the data into training and testing
                train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
                log_messages.append(f"Training data row count: {train_df.count()}")
                log_messages.append(f"Training data columns: {train_df.columns}")
                log_messages.append(f"Testing data row count: {test_df.count()}")
                log_messages.append(f"Testing data columns: {test_df.columns}")

            # Fit the pipeline
            with tracer.start_as_current_span("fit_pipeline"):
                pipe.fit(train_df)
                telemetry.set_span_attribute("training.param_grid", "Fitting done")

            # Model evaluation
            with tracer.start_as_current_span("model_evaluation"):
                # Predict with the pipeline                
                results = pipe.predict(test_df)

                # Use Snowpark ML metrics to calculate MAPE and MSE

                # Calculate MAPE
                mape = mean_absolute_percentage_error(
                    df=results,
                    y_true_col_names=LABEL_COLUMNS,
                    y_pred_col_names=OUTPUT_COLUMNS,
                )

                # Calculate MSE
                mse = mean_squared_error(
                    df=results,
                    y_true_col_names=LABEL_COLUMNS,
                    y_pred_col_names=OUTPUT_COLUMNS,
                )
                telemetry.set_span_attribute("model.mape", mape)
                telemetry.set_span_attribute("model.mse", mse)

            # Model registration
            with tracer.start_as_current_span("model_registration"):

                def set_model_version(registry_object, model_name, major_version=True):
                    # See what we've logged so far, dynamically set the model version
                    import numpy as np
                    import json

                    model_list = registry_object.show_models()

                    if len(model_list) == 0:
                        return "V1"

                    model_list_filter = model_list[model_list["name"] == model_name]

                    if len(model_list_filter) == 0:
                        return "V1"

                    version_list_string = model_list_filter["versions"].iloc[0]
                    version_list = json.loads(version_list_string)
                    version_numbers = [float(s.replace("V", "")) for s in version_list]
                    model_last_version = max(version_numbers)

                    if np.isnan(model_last_version):
                        model_new_version = "V1"

                    elif not np.isnan(model_last_version) and major_version:
                        model_new_version = round(model_last_version + 1, 2)
                        model_new_version = "V" + str(model_new_version)

                    else:
                        model_new_version = round(model_last_version + 0.1, 2)
                        model_new_version = "V" + str(model_new_version)

                    return model_new_version  # This is the version we will use when we log the new model.

                # Create model regisry object
                
                model_registry = registry.Registry(
                    session=session,
                    database_name=session.get_current_database(),
                    schema_name=schema_name,
                )

                
                # Save model to registry
            
                LABEL_COLUMNS = ["CHARGES"]
                FEATURE_COLUMN_NAMES = [
                    i for i in df.schema.names if i not in LABEL_COLUMNS
                ]
                X = train_df.select(FEATURE_COLUMN_NAMES).limit(100)

                model_name = "INSURANCE_CHARGES_PREDICTION"
                version_name = set_model_version(
                    model_registry, model_name, major_version=major_version
                )
                model_version = model_registry.log_model(
                    model=pipe,
                    model_name=model_name,
                    version_name=f'"{version_name}"',
                    sample_input_data=X,
                    conda_dependencies=[
                        "snowflake-snowpark-python",
                        "snowflake-ml-python",
                        "scikit-learn",
                        "xgboost",
                    ],
                )

                model_version.set_metric(metric_name="mean_abs_pct_err", value=mape)
                model_version.set_metric(metric_name="mean_sq_err", value=mse)
                telemetry.add_event("model_registered", {"version": version_name})

                session.sql(
                    f'alter model INSURANCE_CHARGES_PREDICTION set default_version = "{version_name}";'
                )


            print(f"Model {model_name} has been logged with version {version_name} and has a MAPE of {mape} and MSE of {mse}")

        except Exception as e:
            telemetry.add_event(
                "pipeline_failure",
                {"error": str(e), "stack_trace": traceback.format_exc()},
            )
        finally:
            return log_messages


if __name__ == "__main__":
    db_name = valohai.parameters("db_name").value
    schema_name = valohai.parameters("schema_name").value
    warehouse_name = valohai.parameters("warehouse_name").value
    role = valohai.parameters("role").value
    source_table = valohai.parameters("source_table").value

    session = create_session(role, db_name, schema_name, warehouse_name)
    if session is None:
        raise RuntimeError("Failed to create Snowflake session")
    
    print("\nConnection Established with the following parameters:")
    print("Role                        : {}".format(session.get_current_role()))
    print("Database                    : {}".format(session.get_current_database()))
    print("Schema                      : {}".format(session.get_current_schema()))
    print("Warehouse                   : {}".format(session.get_current_warehouse()))

    proc_obj = session.sproc.register(
        func=train_save_ins_model,
        name="train_save_ins_model",
        stage_location="@ML_PIPE_STAGE",
        is_permanent=True,
        replace=True,
        packages=[
            "snowflake-snowpark-python",
            "snowflake-ml-python",
            "xgboost", 
            "pandas",
            "snowflake-telemetry-python",
            "opentelemetry-api",
            "numpy",
            "scikit-learn",  # Explicitly add if using GridSearchCV
        ],
    )
    messages = proc_obj(
        source_table,
        schema_name,
    )
    for message in messages:
        print(message)
