from prefect import flow
from train_model import train


@flow
def train_eval_model_flow():
    train()

if __name__ == '__main__':
    train_eval_model_flow()