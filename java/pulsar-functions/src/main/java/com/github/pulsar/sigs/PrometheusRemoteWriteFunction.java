package com.github.pulsar.sigs;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;
import org.xerial.snappy.Snappy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

class PrometheusRemoteWriteFunction implements Function<byte[], Void> {

    HttpClient client = HttpClient.newBuilder().build();

    @Override
    public Void process(byte[] input, Context context) {
        Logger log = context.getLogger();
        String target = (String) context.getUserConfigValueOrDefault("target", "");
        if(input.length>0 && !target.isBlank()){
            send(input,log,target);
        }
        return null;
    }

    public void send(byte[] data,Logger log,String target){
        HttpRequest.Builder reBuilder = HttpRequest.newBuilder();
        try {
            byte[] compress = Snappy.compress(data);
            HttpRequest request = reBuilder
                    .version(HttpClient.Version.HTTP_1_1)
                    .uri(URI.create(target))
                    .timeout(Duration.ofMillis(5009))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(compress))
                    .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            log.info("remore.write.response:{}",response.statusCode());
        } catch (Exception e) {
            log.error("remote.write.failed!",e);
        }
    }
}