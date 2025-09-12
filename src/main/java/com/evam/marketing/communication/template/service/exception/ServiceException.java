package com.evam.marketing.communication.template.service.exception;

import org.springframework.core.NestedRuntimeException;

public class ServiceException extends NestedRuntimeException {

  public ServiceException(String msg) {
    super(msg);
  }
}
