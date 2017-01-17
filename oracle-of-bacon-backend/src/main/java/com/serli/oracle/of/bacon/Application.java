package com.serli.oracle.of.bacon;

import com.serli.oracle.of.bacon.api.APIEndPoint;

import net.codestory.http.WebServer;

import static com.serli.oracle.of.bacon.utils.EnvUtils.getenv;

public class Application {

    public static void main(String[] args) {
        WebServer webServer = new WebServer();
        APIEndPoint apiEndPoint = new APIEndPoint();
        webServer.configure(routes -> {
            routes.add(apiEndPoint);
        });

        String port = getenv("PORT", "8000");

        webServer.start(Integer.valueOf(port));
        
        // Test MongoDB
        apiEndPoint.getActorByName("De Niro, Robert");
    }
}
