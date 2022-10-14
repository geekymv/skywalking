package org.apache.skywalking.oap.server.webapp.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class Result<T> {

    private int code;

    private T data;

    private String message = "ok";


}
