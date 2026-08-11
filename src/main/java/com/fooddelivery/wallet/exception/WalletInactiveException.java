package com.fooddelivery.wallet.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.FORBIDDEN)
public class WalletInactiveException extends RuntimeException {
    public WalletInactiveException(String message) {
        super(message);
    }
}
