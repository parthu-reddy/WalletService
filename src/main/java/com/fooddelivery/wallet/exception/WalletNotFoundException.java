package com.fooddelivery.wallet.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class WalletNotFoundException extends com.fooddelivery.common.exception.ResourceNotFoundException {
    public WalletNotFoundException(String message) {
        super(message);
    }
}
