import json
import pandas as pd
import pickle
import valohai

from valohai.paths import get_outputs_path
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder


def load_data():
    """Load Iris dataset from Valohai inputs"""
    # Valohai input files are available in the inputs directory
    dataset_path = valohai.inputs("iris_dataset").path()
    print(f"Loading Iris dataset from: {dataset_path}")
    
    # Iris dataset doesn't have headers, so we'll add them
    column_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
    
    try:
        # Try loading with headers first (in case user added them)
        df = pd.read_csv(dataset_path)
        if df.shape[1] != 5:
            raise ValueError("Unexpected number of columns")
        
        # Check if headers are already present
        if df.iloc[0].dtype == 'object' and any(df.iloc[0].astype(str).str.contains('sepal|petal|species', case=False)):
            print("Headers detected in dataset")
            return df
        else:
            # No headers, add them
            df.columns = column_names
            return df
            
    except Exception:
        # Load without headers and add them
        df = pd.read_csv(dataset_path, header=None, names=column_names)
        print("Added column names to headerless Iris dataset")
        return df


def preprocess_data(df, config):
    """Preprocess the data"""
    target_col = config.get("target_column", "target")
    feature_cols = config.get("feature_columns")
    
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found in dataset")
    
    # Select features
    if feature_cols is None:
        feature_cols = [col for col in df.columns if col != target_col]
    
    X = df[feature_cols]
    y = df[target_col]
    
    # Handle categorical features (simple label encoding)
    le_dict = {}
    for col in X.columns:
        if X[col].dtype == 'object':
            le = LabelEncoder()
            X[col] = le.fit_transform(X[col].astype(str))
            le_dict[col] = le
    
    # Handle categorical target
    y_le = None
    if y.dtype == 'object':
        y_le = LabelEncoder()
        y = y_le.fit_transform(y.astype(str))
    
    return X, y, le_dict, y_le

def train_model(X, y, learning_rate, epochs, batch_size, random_state=42):
    """Train the model for Iris classification"""
    # For Iris dataset, we'll use RandomForest where:
    # - epochs = n_estimators (number of trees)
    # - learning_rate affects max_features (as a ratio)
    # - batch_size doesn't apply to RF, but we'll log it
    
    # Calculate max_features from learning_rate (as a ratio of total features)
    max_features = max(1, int(learning_rate * X.shape[1]))
    
    print("Training Random Forest for Iris classification:")
    print(f"  Number of trees (epochs): {epochs}")
    print(f"  Max features (from learning_rate): {max_features}")
    print(f"  Batch size (not applicable): {batch_size}")
    print(f"  Features: {X.shape[1]}, Samples: {X.shape[0]}")
    
    # Split data (80/20 split)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )
    
    print(f"Train set: {X_train.shape[0]} samples, Test set: {X_test.shape[0]} samples")
    
    # Train model
    model = RandomForestClassifier(
        n_estimators=epochs,
        max_features=max_features,
        random_state=random_state,
        n_jobs=-1
    )
    
    print("Starting training...")
    model.fit(X_train, y_train)
    
    # Evaluate
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    
    print(f"Training accuracy: {train_score:.4f}")
    print(f"Test accuracy: {test_score:.4f}")
    
    # Get predictions and detailed metrics
    y_pred = model.predict(X_test)
    
    # Feature importance for Iris
    feature_names = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
    feature_importance = dict(zip(feature_names, model.feature_importances_))
    
    print("Feature importances:")
    for feature, importance in feature_importance.items():
        print(f"  {feature}: {importance:.4f}")
    
    return model, {
        'train_accuracy': train_score,
        'test_accuracy': test_score,
        'feature_importance': feature_importance,
        'classification_report': classification_report(y_test, y_pred, output_dict=True),
        'dataset': 'iris',
        'model_type': 'RandomForestClassifier',
        'n_estimators': epochs,
        'max_features': max_features
    }

def save_outputs(model, metrics, le_dict, y_le, outputs_dir):
    """Save model and results to Valohai outputs"""
    
    # Save the trained model
    model_path = f"{outputs_dir}/model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'label_encoders': le_dict,
            'target_encoder': y_le
        }, f)
    print(f"Model saved to: {model_path}")
    
    # Save metrics
    metrics_path = f"{outputs_dir}/model.metadata.json"
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics saved to: {metrics_path}")
    
def main():
    learning_rate = valohai.parameters("learning_rate").value
    epochs = valohai.parameters("epochs").value
    batch_size = valohai.parameters("batch_size").value
    config = {
            "target_column": "species",  # Iris target column
            "feature_columns": ["sepal_length", "sepal_width", "petal_length", "petal_width"],
            "random_state": 42
        }

    try:
        
        # Load data and config
        df = load_data()
        print(f"Dataset shape: {df.shape}")

        # Preprocess
        X, y, le_dict, y_le = preprocess_data(df, config)
        
        # Train model
        model, metrics = train_model(
            X, y, 
            learning_rate,
            epochs,
            batch_size,
            config['random_state'],
        )
        
        # Save outputs
        outputs_dir = get_outputs_path()
        save_outputs(model, metrics, le_dict, y_le, outputs_dir)

        print("Training completed successfully!")

        # Log final metrics for Valohai to capture
        print(f"FINAL_ACCURACY: {metrics['test_accuracy']:.4f}")
        
    except Exception as e:
        print(f"Training failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()