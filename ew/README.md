BLOX is a block server library for Ergo.

It takes care of:
    - retrieving blocks along the main chain
    - expanding the inputs into boxes
    - decoding addresses and registers

It is made for use cases where each box needs only be seen once, while keeping storage needs to a minimum.
