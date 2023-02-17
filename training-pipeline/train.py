import logging

import joblib
import lightgbm as lgb
import pandas as pd
import wandb

import preprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WANDB_ENTITY = "p-b-iusztin"
WANDB_PROJECT = "energy_consumption"

# sweep_configs = {
#     "method": "grid",
#     "metric": {"name": "validation.rmse", "goal": "minimize"},
#     "parameters": {
#         "n_estimators": {"values": [100, 500, 1000]},
#         "max_depth": {"values": [3, 5, 7]},
#         "learning_rate": {"values": [0.05, 0.1, 0.2]},
#         "bagging_fraction": {"values": [0.8, 1.0]},
#         "feature_fraction": {"values": [0.8, 1.0]},
#         "lambda_l2": {"values": [0.0, 0.01]},
#     },
# }
sweep_configs = {
    "method": "grid",
    "metric": {"name": "validation.rmse", "goal": "minimize"},
    "parameters": {
        "n_estimators": {"values": [100]},
        "max_depth": {"values": [1, 2, 3]},
        "learning_rate": {"values": [0.05]},
        "bagging_fraction": {"values": [0.8]},
        "feature_fraction": {"values": [0.8]},
        "lambda_l2": {"values": [0.0]},
    },
}
sweep_id = wandb.sweep(sweep=sweep_configs, project="energy_consumption")


def find_best_hyperparameters(
    data_path: str = "../energy_consumption_data.parquet",
    model_path: str = "../energy_consumption_model.pkl",
    target: str = "energy_consumption_future_hours_0",
) -> None:
    """
    Template for training a model.

    Args:
        data_path: Path to the training data.
        model_path: Path to save the trained model.
    """
    # load data
    df = load_data_from_parquet(data_path)
    # preprocess data
    train_df, validation_df, test_df = preprocess.split_data(df)
    train_df, validation_df, test_df = preprocess.encode_categorical(
        train_df, validation_df, test_df
    )

    metadata = {
        "features": list(df.columns),
    }
    with init_wandb_run(name="feature_view", job_type="upload_feature_view") as run:
        raw_data_at = wandb.Artifact(
            "energy_consumption_data",
            type="feature_view",
            metadata=metadata,
        )
        run.log_artifact(raw_data_at)

    with init_wandb_run(name="train_validation_test_split", job_type="split") as run:
        data_at = run.use_artifact(
            "energy_consumption_data:latest"
        )
        data_dir = data_at.download()

        artifacts = {}
        for split in ["train", "validation", "test"]:
            split_df = locals()[f"{split}_df"]
            starting_datetime = split_df["datetime_utc"].min()
            ending_datetime = split_df["datetime_utc"].max()
            metadata = {
                "starting_datetime": starting_datetime,
                "ending_datetime": ending_datetime,
            }

            artifacts[split] = wandb.Artifact(
                f"{split}_split",
                type="split",
                metadata=metadata
            )

        for split, artifact in artifacts.items():
            run.log_artifact(artifact)

    train_experiment(
        train_df=train_df,
        validation_df=validation_df,
        target=target,
    )


def load_data_from_parquet(data_path: str) -> pd.DataFrame:
    """
    Template for loading data from a parquet file.

    Args:
        data_path: Path to the parquet file.

    Returns: Dataframe with the data.
    """

    return pd.read_parquet(data_path)


def train_experiment(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    target: str,
) -> lgb.LGBMRegressor:
    """
    Function that is training a LGBM model.

     Args:
         df: Dataframe containing the training data.
         target: Name of the target column.

     Returns: Trained LightGBM model
    """

    with init_wandb_run(name="experiment", job_type="hpo") as run:
        config = wandb.config
        data_at = run.use_artifact(
            f"{WANDB_ENTITY}/{WANDB_PROJECT}/test_split:latest", type="split"
        )
        data_at = run.use_artifact(
            f"{WANDB_ENTITY}/{WANDB_PROJECT}/train_split:latest", type="split"
        )

        model = train(df=train_df, target=target, config=config)

        # evaluate model
        rmse = evaluate_model(
            model, train_df, target=target
        )
        logger.info(f"Train RMSE: {rmse:.2f}")
        logger.info(
            f"Train Mean Energy Consumption: {train_df['energy_consumption_future_hours_0'].mean():.2f}"
        )
        wandb.log({"train": {"rmse": rmse}})

        rmse = evaluate_model(
            model, validation_df, target=target
        )
        logger.info(f"Train RMSE: {rmse:.2f}")
        logger.info(
            f"Train Mean Energy Consumption: {validation_df['energy_consumption_future_hours_0'].mean():.2f}"
        )
        wandb.log({"validation": {"rmse": rmse}})

    return model


def train(df: pd.DataFrame, target: str, config: dict):
    model = lgb.LGBMRegressor(
        n_estimators=config["n_estimators"],
        max_depth=config["max_depth"],
        learning_rate=config["learning_rate"],
        bagging_fraction=config["bagging_fraction"],
        feature_fraction=config["feature_fraction"],
        lambda_l2=config["lambda_l2"],
        n_jobs=-1,
        random_state=42,
    )

    feature_columns = list(set(df.columns) - set([target, "datetime_utc"]))
    model.fit(X=df[feature_columns], y=df[target])

    return model



# write a function that evaluates the model with rmse
def evaluate_model(model, df: pd.DataFrame, target: str):
    """
    Template for evaluating a model.

    Args:
        model: Trained model.
        df: Dataframe containing the evaluation data.
        target: Name of the target column.

    Returns: RMSE
    """
    from sklearn.metrics import mean_squared_error

    feature_columns = list(set(df.columns) - set([target, "datetime_utc"]))
    y_pred = model.predict(df[feature_columns])
    y_true = df[target]

    return mean_squared_error(y_true, y_pred, squared=False)


# write function that saves the model using joblib
def save_model(model, model_path: str):
    """
    Template for saving a model.

    Args:
        model: Trained model.
        model_path: Path to save the model.
    """

    joblib.dump(model, model_path)


def init_wandb_run(
    name: str, project: str = WANDB_PROJECT, entity: str = WANDB_ENTITY, **kwargs
):
    name = f"{name}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}"

    run = wandb.init(project=project, entity=entity, name=name, **kwargs)

    return run


def train_best_model(
    data_path: str = "../energy_consumption_data.parquet",
    model_path: str = "../energy_consumption_model.pkl",
    target: str = "energy_consumption_future_hours_0",
) -> None:
    """
    Template for training a model.

    Args:
        data_path: Path to the training data.
        model_path: Path to save the trained model.
    """

    api = wandb.Api()
    sweep = api.sweep(f"{WANDB_ENTITY}/{WANDB_PROJECT}/sweeps/{sweep_id}")
    best_run = sweep.best_run()
    config = best_run.config

    # load data
    df = load_data_from_parquet(data_path)
    # preprocess data
    train_df, validation_df, test_df = preprocess.split_data(df)
    train_df, validation_df, test_df = preprocess.encode_categorical(
        train_df, validation_df, test_df
    )

    # train on train + validation split to compute test score
    model = train(
        df=pd.concat([train_df, validation_df], axis=0),
        target=target,
        config=config
    )

    rmse = evaluate_model(
        model, test_df, target="energy_consumption_future_hours_0"
    )
    logger.info(f"Test RMSE: {rmse:.2f}")
    logger.info(
        f"Test Mean Energy Consumption: {test_df['energy_consumption_future_hours_0'].mean():.2f}"
    )

    # train on everything for the final model
    with init_wandb_run(name="best_model", job_type="best_model") as run:
        model = train(
            df=pd.concat([train_df, validation_df, test_df], axis=0),
            target=target,
            config=config
        )
        save_model(model, model_path)

        metadata = dict(config)
        metadata["target"] = target
        metadata["test_rmse"] = rmse
        description = f"""
        LightGBM Regressor trained on the whole dataset with the best hyperparameters found by wandb sweep. 
        The model is trained on {target} as target.
        """
        model_artifact = wandb.Artifact(
            "LGBMRegressor",
            type="model",
            description=description,
            metadata=metadata
        )
        # wandb.save(model_path)
        model_artifact.add_file(model_path)

        run.log_artifact(model_artifact)


if __name__ == "__main__":
    wandb.agent(project="energy_consumption", sweep_id=sweep_id, function=find_best_hyperparameters)
    train_best_model()


