def circ_supply(height: int) -> int:
    """
    Circulating supply at given height, in ERG.
    """
    # Emission settings
    initial_rate = 75
    fixed_rate_blocks = 525600 - 1
    epoch_length = 64800
    step = 3

    # At current height
    completed_epochs = max(0, height - fixed_rate_blocks) // epoch_length
    current_epoch = completed_epochs + min(1, completed_epochs)
    blocks_in_current_epoch = max(0, height - fixed_rate_blocks) % epoch_length
    current_rate = max(0, initial_rate - current_epoch * step)

    # Components
    fixed_period_cs = min(fixed_rate_blocks, height) * initial_rate
    completed_epochs_cs = sum(
        [
            epoch_length * max(0, initial_rate - step * (i + 1))
            for i in range(completed_epochs)
        ]
    )
    current_epoch_cs = blocks_in_current_epoch * current_rate

    # Circulating supply
    return fixed_period_cs + completed_epochs_cs + current_epoch_cs


