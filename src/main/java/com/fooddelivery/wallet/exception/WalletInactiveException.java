package com.fooddelivery.wallet.exception;

public class WalletInactiveException extends RuntimeException {
    public WalletInactiveException(String message) {
        super(message);
    }
}
