import time
import os.path as osp

import numpy as np

import torch
import torch.nn.functional as F


def train(
    model,
    loss_fn,
    train_dataloader,
    optimizer,
    device,
    model_path,
    model_name,
    epochs=4,
    patience=10,
    eval=False,
    loss_fn_val=None,
    val_dataloader=None,
):
    print(
        f"{'Epoch':^7} | {'Batch':^7} | {'Train Loss':^12} | "
        f"{'Val Loss':^10} | {'Val Acc':^9} | {'Elapsed':^9}"
    )
    print("-" * 70)

    trigger_time = 0
    last_loss = np.inf
    best_model = None

    for epoch in range(epochs):
        # Measure the elapsed time of each epoch
        t0_epoch, t0_batch = time.time(), time.time()

        # Reset tracking variables at the beginning of each epoch
        total_loss, batch_loss, batch_counts = 0, 0, 0

        # Put the model into training mode
        model.train()

        # For each batch ...
        for step, batch in enumerate(train_dataloader):
            batch_counts += 1

            # Zero out previously calculated gradients
            optimizer.zero_grad()

            # Load batch to GPU
            batch = batch.to(device)

            # Compute loss and accumulate the loss values
            output = model(batch.x, batch.edge_index)
            target = batch.y

            # CrossEntropyLoss already apply softmax
            loss = loss_fn(output, target)

            batch_loss += loss.item()
            total_loss += loss.item()

            loss.backward()
            optimizer.step()

            # Log the loss values and elapsed time for every 20 batches
            if (step % 500 == 0 and step != 0) or (step == len(train_dataloader) - 1):
                # Calculate time elapsed for 20 batches
                time_elapsed = time.time() - t0_batch

                # Training results
                print(
                    f"{epoch + 1:^7} | {step:^7} | {batch_loss / batch_counts:^12.6f} | {'-':^10} | "
                    f"{'-':^9} | {time_elapsed:^9.2f}"
                )

                batch_loss, batch_counts = 0, 0
                t0_batch = time.time()

        avg_train_loss = total_loss / len(train_dataloader)
        print("-" * 70)

        if eval:
            # After the completion of each training epoch, measure the model's perfomance
            # on validation set.
            val_loss, val_accuracy = evaluate(
                model, val_dataloader, loss_fn_val, device
            )

            # Log performance over the entire training data
            time_elapsed = time.time() - t0_epoch

            print(
                f"{epoch + 1:^7} | {'-':^7} | {avg_train_loss:^12.6f} | {val_loss:^10.6f} | "
                f"{val_accuracy:^9.2f} | {time_elapsed:^9.2f}"
            )

            print("-" * 70)

            # Early stopping
            if val_loss > last_loss:
                trigger_time += 1
            else:
                last_loss = val_loss
                best_model = model.state_dict()
                trigger_time = 0

            if trigger_time > patience:
                torch.save(best_model, osp.join(model_path, f"{model_name}.h5"))
                break
        else:
            # Early stopping
            if avg_train_loss > last_loss:
                trigger_time += 1
            else:
                last_loss = avg_train_loss
                best_model = model.state_dict()
                trigger_times = 0

            if trigger_times >= patience:
                torch.save(best_model, osp.join(model_path, f"{model_name}.h5"))
                break

        torch.save(
            model.state_dict() if best_model is None else best_model,
            osp.join(model_path, f"{model_name}.h5"),
        )


def evaluate(model, val_dataloader, loss_fn, device):
    # Put the model into the evaluation mode. The dropout layers are disabled during the test time.
    model.eval()

    # Tracking variables
    val_accuracy = []
    val_loss = []

    # For each batch in our validation set...
    for batch in val_dataloader:
        # Load batch to GPU
        batch = batch.to(device)

        # Compute logits
        with torch.no_grad():
            output = model(batch.x, batch.edge_index)

            # Compute loss
            loss = loss_fn(output, batch.y)
            val_loss.append(loss.item())

            # Get the predictions
            probs = F.softmax(output, dim=1)
            preds = torch.argmax(probs, dim=1)
            # Calculate the accuracy rate
            accuracy = (preds == batch.y).cpu().numpy().mean() * 100

            val_accuracy.append(accuracy)

    # Compute the average accuracy and loss over the validation set.
    val_loss = np.mean(val_loss)
    val_accuracy = np.mean(val_accuracy)

    return val_loss, val_accuracy


def predict(model, test_dataloader, device):
    # Put the model into the evaluation mode. The dropout layers are disabled during the test time.
    model.eval()
    # Init outputs
    outputs = []
    y_true = []

    # For each batch in our validation set...
    for batch in test_dataloader:
        #  Load batch to GPU
        batch = batch.to(device)
        # Compute logits
        with torch.no_grad():
            output = model(batch.x, batch.edge_index)
        y_true.append(batch.y)
        outputs.append(output)

    # Concatenate logits from each batch
    outputs = torch.cat(outputs, dim=0)
    y_true = torch.cat(y_true, dim=0)

    # Apply softmax to calculate probabilities
    probs = F.softmax(outputs, dim=1).cpu().numpy()
    y_pred = np.argmax(probs, axis=1)
    y_true = y_true.cpu().numpy()

    return y_true, y_pred
