import os, json, time
import numpy as np
import torch

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def export_bundle(model, window_size, mean, std, threshold, out_dir, feature_columns=None, model_name=None, version=None):
    ensure_dir(out_dir)

    if model_name is None:
        model_name = os.getenv("AE_MODEL_NAME", "anomaly-detector-autoencoder")
    if version is None:
        version = os.path.basename(out_dir)

    # 1) Export ONNX
    model.eval()
    dummy = torch.zeros(1, window_size, dtype=torch.float32)
    onnx_path = os.path.join(out_dir, "model.onnx")

    # IMPORTANT: use input/output names that match edge code: input/output
    torch.onnx.export(
        model,
        dummy,
        onnx_path,
        input_names=["input"],
        output_names=["output"],
        dynamic_axes={"input": {0: "batch"}, "output": {0: "batch"}},
        opset_version=17
    )

    # 2) preprocess.json
    # mean/std can be scalar (windowed) OR list (pivoted)
    preprocess = {
        "window_size": int(window_size),
        "normalization": "zscore",
        "mean": mean,
        "std": std,
    }
    if feature_columns is not None:
        preprocess["feature_columns"] = feature_columns

    with open(os.path.join(out_dir, "preprocess.json"), "w") as f:
        json.dump(preprocess, f, indent=2)

    # 3) thresholds.json
    thresholds = {
        "type": "mse_reconstruction",
        "mse_threshold": float(threshold),
        # keep backward-compatible key too
        "threshold": float(threshold),
    }
    with open(os.path.join(out_dir, "thresholds.json"), "w") as f:
        json.dump(thresholds, f, indent=2)

    # 4) metadata.json
    metadata = {
        "model_name": model_name,
        "version": version,
        "exported_at": int(time.time()),
        "format": "onnx",
        "opset": 17,
    }
    if feature_columns is not None:
        metadata["n_features"] = len(feature_columns)

    with open(os.path.join(out_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Exported model bundle to: {out_dir}")
    print(f" - {onnx_path}")