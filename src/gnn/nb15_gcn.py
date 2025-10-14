import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn.conv import GCN2Conv


class NB15_GCN(nn.Module):
    def __init__(
        self,
        n_features,
        n_classes,
        n_convs,
        n_hidden,
        dropout,
        alpha,  # strength of initial residual connection
        theta,  # hyperparameter to compute the strength of the identity mapping
    ):
        super(NB15_GCN, self).__init__()

        self.n_hidden = n_hidden
        self.n_convs = n_convs

        self.linear = nn.Linear(n_features, n_hidden)
        self.convs = nn.ModuleList(
            [
                GCN2Conv(
                    channels=n_hidden,
                    alpha=alpha,
                    theta=theta,
                    layer=layer_idx + 1,
                    normalize=True,
                )
                for layer_idx in range(n_convs)
            ]
        )

        self.norm = nn.LayerNorm(n_hidden, elementwise_affine=True)
        self.dropout = nn.Dropout(p=dropout)

        self.fc1 = nn.Linear(n_hidden, n_hidden)
        self.fc2 = nn.Linear(n_hidden, n_classes)

    def forward(self, x, edge_index):
        x = self.linear(x)
        x0 = x  # residual signal

        for conv in self.convs:
            x = self.dropout(x)
            x = F.gelu(conv(x, x0, edge_index))

        x = F.gelu(self.norm(x))
        x = self.dropout(x)

        x = F.gelu(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)

        return x
