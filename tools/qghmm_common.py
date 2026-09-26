"""Shared NumPy implementation for float and quantized QG-HMM tools."""
import math

import numpy as np
import pandas as pd


def logsumexp(values, axis=-1):
    maximum = np.max(values, axis=axis, keepdims=True)
    return np.squeeze(maximum, axis=axis) + np.log(
        np.exp(values - maximum).sum(axis=axis) + 1e-30)


def weighted_gaussians(x, y, weights, classes, variance_floor=0.05):
    means, variances, priors = [], [], []
    for label in classes:
        mask = y == label
        class_x = x[mask]
        class_w = weights[mask]
        total = max(float(class_w.sum()), 1e-12)
        mean = (class_x * class_w[:, None]).sum(axis=0) / total
        variance = ((class_x - mean) ** 2 * class_w[:, None]).sum(axis=0) / total
        means.append(mean)
        variances.append(np.maximum(variance, variance_floor))
        priors.append(total)
    priors = np.asarray(priors, dtype=float)
    priors /= priors.sum()
    return {
        "means": np.asarray(means).tolist(),
        "variances": np.asarray(variances).tolist(),
        "log_priors": np.log(priors + 1e-30).tolist(),
        "variance_floor": variance_floor,
    }


def learn_transitions(frame, classes, smoothing=1.0):
    index = {label: i for i, label in enumerate(classes)}
    counts = np.full((len(classes), len(classes)), smoothing, dtype=float)
    ordered = frame.sort_values(["run_id", "link_id", "timestamp_ns"])
    for _, group in ordered.groupby(["run_id", "link_id"], sort=False):
        labels = group["fault_class"].tolist()
        for previous, current in zip(labels, labels[1:]):
            if previous in index and current in index:
                counts[index[previous], index[current]] += 1.0
    probabilities = counts / counts.sum(axis=1, keepdims=True)
    return {"log_transition": np.log(probabilities + 1e-30).tolist(),
            "smoothing": smoothing}


def train_gmm(x, y, weights, classes, components=2, variance_floor=0.05,
              iterations=30, seed=2026):
    rng = np.random.default_rng(seed)
    all_means, all_vars, all_mix = [], [], []
    for label in classes:
        mask = y == label
        values, sample_w = x[mask], weights[mask]
        if len(values) == 0:
            raise ValueError(f"no samples for {label}")
        if len(values) < components:
            initial = np.repeat(values[:1], components, axis=0)
        else:
            initial = values[rng.choice(len(values), components, replace=False)]
        means = initial.copy()
        variances = np.repeat(np.maximum(np.var(values, axis=0), variance_floor)[None, :],
                              components, axis=0)
        mixture = np.full(components, 1.0 / components)
        for _ in range(iterations):
            diff = values[:, None, :] - means[None, :, :]
            scores = (-0.5 * (np.log(2 * np.pi * variances)[None, :, :]
                              + diff * diff / variances[None, :, :]).sum(axis=2)
                      + np.log(mixture + 1e-30)[None, :])
            responsibilities = np.exp(scores - logsumexp(scores, axis=1)[:, None])
            responsibilities *= sample_w[:, None]
            mass = responsibilities.sum(axis=0) + 1e-12
            means = (responsibilities.T @ values) / mass[:, None]
            diff = values[:, None, :] - means[None, :, :]
            variances = ((responsibilities[:, :, None] * diff * diff).sum(axis=0)
                         / mass[:, None])
            variances = np.maximum(variances, variance_floor)
            mixture = mass / mass.sum()
        all_means.append(means.tolist())
        all_vars.append(variances.tolist())
        all_mix.append(mixture.tolist())
    return {"components": components, "means": all_means,
            "variances": all_vars, "log_mixture": np.log(np.asarray(all_mix) + 1e-30).tolist(),
            "variance_floor": variance_floor}


def gaussian_emissions(x, model):
    means = np.asarray(model["means"], dtype=float)
    variances = np.asarray(model["variances"], dtype=float)
    diff = x[:, None, :] - means[None, :, :]
    return -0.5 * (np.log(2 * np.pi * variances)[None, :, :]
                   + diff * diff / variances[None, :, :]).sum(axis=2)


def gmm_emissions(x, model):
    means = np.asarray(model["means"], dtype=float)
    variances = np.asarray(model["variances"], dtype=float)
    log_mix = np.asarray(model["log_mixture"], dtype=float)
    diff = x[:, None, None, :] - means[None, :, :, :]
    component = (-0.5 * (np.log(2 * np.pi * variances)[None, :, :, :]
                         + diff * diff / variances[None, :, :, :]).sum(axis=3)
                 + log_mix[None, :, :])
    return logsumexp(component, axis=2)


def sequence_scores(frame, emissions, classes, transition=None, priors=None):
    scores = np.zeros_like(emissions)
    if priors is None:
        priors = np.full(len(classes), -math.log(len(classes)))
    transition = None if transition is None else np.asarray(transition, dtype=float)
    ordered_indices = []
    for _, group in frame.groupby(["run_id", "link_id"], sort=False):
        indices = list(group.index)
        ordered_indices.extend(indices)
        state = None
        for index in indices:
            emission = emissions[index]
            if state is None or transition is None:
                state = emission + priors
            else:
                state = emission + np.max(state[:, None] + transition, axis=0)
            state = state - np.max(state)
            scores[index] = state
    return scores


def predict_float(frame, features, classes, model, kind):
    ordered = frame.reset_index(drop=True)
    x = ordered[features].to_numpy(dtype=float)
    if kind in {"M1", "M2"}:
        emissions = gaussian_emissions(x, model["emission"])
    elif kind == "M3":
        emissions = gmm_emissions(x, model["emission"])
    elif kind == "M4":
        centers = np.asarray(model["centers"], dtype=float)
        distance = ((x[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
        design = np.exp(-float(model["gamma"]) * distance)
        scores = design @ np.asarray(model["coefficients"], dtype=float)
        return scores, scores.max(axis=1)
    else:
        raise ValueError(kind)
    transition = model.get("log_transition") if kind in {"M2", "M3"} else None
    priors = np.asarray(model.get("log_priors",
                                  [-math.log(len(classes))] * len(classes)))
    scores = sequence_scores(ordered, emissions, classes, transition, priors)
    return scores, emissions.max(axis=1)


def train_sparse_rbf(x, y, weights, classes, max_centers=64, ridge=1e-2, seed=2026):
    rng = np.random.default_rng(seed)
    per_class = max(1, max_centers // len(classes))
    selected = []
    for label in classes:
        candidates = np.flatnonzero(y == label)
        choice = rng.choice(candidates, min(per_class, len(candidates)), replace=False)
        selected.extend(choice.tolist())
    centers = x[selected]
    if len(centers) > 1:
        pairs = centers[:min(64, len(centers))]
        distances = ((pairs[:, None, :] - pairs[None, :, :]) ** 2).sum(axis=2)
        positive = distances[distances > 0]
        gamma = 1.0 / max(float(np.median(positive)), 1e-3)
    else:
        gamma = 1.0
    design = np.exp(-gamma * ((x[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2))
    targets = np.zeros((len(x), len(classes)))
    class_index = {label: i for i, label in enumerate(classes)}
    for row, label in enumerate(y):
        targets[row, class_index[label]] = 1.0
    weighted_design = design * weights[:, None]
    lhs = design.T @ weighted_design + ridge * np.eye(design.shape[1])
    rhs = weighted_design.T @ targets
    coefficients = np.linalg.solve(lhs, rhs)
    return {"centers": centers.tolist(), "gamma": gamma,
            "coefficients": coefficients.tolist(), "ridge": ridge,
            "interpretation": "sparse RBF/Nystrom GP-like offline upper bound"}


def class_predictions(scores, classes):
    indices = np.argmax(scores, axis=1)
    return np.asarray([classes[index] for index in indices]), indices


def macro_f1(y_true, y_pred, classes):
    values = []
    for label in classes:
        tp = np.sum((y_true == label) & (y_pred == label))
        fp = np.sum((y_true != label) & (y_pred == label))
        fn = np.sum((y_true == label) & (y_pred != label))
        precision = tp / max(1, tp + fp)
        recall = tp / max(1, tp + fn)
        values.append(2 * precision * recall / max(1e-12, precision + recall))
    return float(np.mean(values))


def softmax(scores):
    shifted = scores - scores.max(axis=1, keepdims=True)
    values = np.exp(shifted)
    return values / values.sum(axis=1, keepdims=True)

