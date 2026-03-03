import torch
from torch import nn


class Autoencoder(nn.Module):
    def __init__(self, input_size: int, hidden1: int = 5, hidden2: int = 2):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_size, hidden1),
            nn.ReLU(),
            nn.Linear(hidden1, hidden2),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(hidden2, hidden1),
            nn.ReLU(),
            nn.Linear(hidden1, input_size),
        )

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)
