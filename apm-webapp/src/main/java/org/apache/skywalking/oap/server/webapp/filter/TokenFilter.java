package org.apache.skywalking.oap.server.webapp.filter;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.webapp.model.Result;
import org.apache.skywalking.oap.server.webapp.service.TokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
@Component
public class TokenFilter implements GlobalFilter, Ordered {

    private static final String TOKEN = "token";

    private Gson gson = new Gson();

    @Autowired
    private TokenService tokenService;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        try {
            URL url = request.getURI().toURL();
            log.info("url = {}", url);
        } catch (MalformedURLException e) {
            log.error(e.getMessage());
        }

        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);

        String token = getRequestToken(request);
        if (!StringUtils.hasLength(token)) {
            Result res = Result.builder()
                    .code(4000)
                    .message("token is empty")
                    .build();

            return this.sendResponseJson(response, res);
        }

        // 验证 token 是否合法
        if (!tokenService.check(token)) {
            Result res = Result.builder()
                    .code(4001)
                    .message("token invalid")
                    .build();

            return this.sendResponseJson(response, res);
        }

        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return -1;
    }

    private String getRequestToken(ServerHttpRequest req) {
        List<String> tokens = req.getHeaders().get(TOKEN);
        String token = "";
        if (!CollectionUtils.isEmpty(tokens)) {
            token = tokens.get(0);
        }
        if (!StringUtils.hasLength(token)) {
            token = req.getQueryParams().getFirst(TOKEN);
        }
        return token;
    }

    private Mono<Void> sendResponseJson(ServerHttpResponse response, Result res) {
        String result = gson.toJson(res);
        DataBuffer buffer = response.bufferFactory().wrap(result.getBytes(StandardCharsets.UTF_8));
        return response.writeWith(Mono.just(buffer));
    }

}
