"""
Pan-STARRS triplet CNN architecture (target / ref / diff).

Shared by training (7.CNN.py) and production inference (cnn_inference.py).
"""

from __future__ import annotations

import torch
import torch.nn as nn

from cnn_data import IMAGE_SIZE


class PanSTARRSCNN(nn.Module):
    """Encoder-style conv stack + dense head (matches the legacy Keras encoder)."""

    def __init__(self, in_channels: int = 3) -> None:
        super().__init__()

        def conv_block(in_ch: int, out_ch: int, dilation: int = 1) -> nn.Sequential:
            padding = dilation
            return nn.Sequential(
                nn.Conv2d(in_ch, out_ch, kernel_size=3, padding=padding, dilation=dilation),
                nn.LeakyReLU(0.01, inplace=True),
                nn.Dropout2d(0.1),
            )

        self.conv2 = conv_block(in_channels, 8)
        self.pool2 = nn.MaxPool2d(2)
        self.conv3 = conv_block(8, 16)
        self.pool3 = nn.MaxPool2d(2)
        self.conv4 = conv_block(16, 32)
        self.pool4 = nn.MaxPool2d(2)
        self.conv5 = conv_block(32, 64, dilation=2)
        self.pool5 = nn.MaxPool2d(2)
        self.conv6 = conv_block(64, 128)
        self.pool6 = nn.MaxPool2d(2)
        self.conv7 = conv_block(128, 256)
        self.pool7 = nn.MaxPool2d(3)

        with torch.no_grad():
            dummy = torch.zeros(1, in_channels, IMAGE_SIZE, IMAGE_SIZE)
            flat_dim = self._forward_features(dummy).shape[1]

        self.head = nn.Sequential(
            nn.Linear(flat_dim, 32),
            nn.LeakyReLU(0.01, inplace=True),
            nn.Dropout(0.1),
            nn.Linear(32, 16),
            nn.LeakyReLU(0.01, inplace=True),
            nn.Linear(16, 1),
        )

    def _forward_features(self, x: torch.Tensor) -> torch.Tensor:
        x = self.pool2(self.conv2(x))
        x = self.pool3(self.conv3(x))
        x = self.pool4(self.conv4(x))
        x = self.pool5(self.conv5(x))
        x = self.pool6(self.conv6(x))
        x = self.pool7(self.conv7(x))
        return torch.flatten(x, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(self._forward_features(x))
