import os, json, time
import numpy as np
import torch

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def export_bundle(model, window_size, mean, std, threshold, out_dir):
    ensure_dir(out_dir)

    # 1) Export ONNX
    model.eval()
    dummy = torch.zeros(1, window_size, dtype=torch.float32)
    onnx_path = os.path.join(out_dir, "model.onnx")

    torch.onnx.export(
        model,
        dummy,
        onnx_path,
        input_names=["x"],
        output_names=["recon"],
        dynamic_axes={"x": {0: "batch"}, "recon": {0: "batch"}},
        opset_version=17
    )

    # 2) preprocess.json
    preprocess = {
        "window_size": int(window_size),
        "normalization": "zscore",
        "mean": float(mean),
        "std": float(std),
    }
    with open(os.path.join(out_dir, "preprocess.json"), "w") as f:
        json.dump(preprocess, f, indent=2)

    # 3) thresholds.json
    thresholds = {
        "type": "mse_reconstruction",
        "threshold": float(threshold),
    }
    with open(os.path.join(out_dir, "thresholds.json"), "w") as f:
        json.dump(thresholds, f, indent=2)

    # 4) metadata.json
    metadata = {
        "model_name": "anomaly-detector-autoencoder",
        "version": os.path.basename(out_dir),
        "exported_at": int(time.time()),
        "format": "onnx",
        "opset": 17
    }
    with open(os.path.join(out_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Exported model bundle to: {out_dir}")
    print(f" - {onnx_path}")

if __name__ == "__main__":

    print("Run this by importing export_bundle from train.py")
