package org.apache.skywalking.oap.server.webapp.service;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class TokenService {

    public boolean check(String token) {

        if (!StringUtils.hasLength(token)) {
            return false;
        }

        // TODO 调用 token 服务

        return true;
    }


}
