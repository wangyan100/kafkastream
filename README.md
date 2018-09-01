

## README
- This kafka streams applciation can read xml format message from kafka source topic and based  xml message tag's content to pick up matched message and dispatch it to  destination topic. For using this application, you have to define configs at config.properties file.

      

- config files

  - jaas.conf is from system level for jaas config

  - log4j.properties is for logging config

  - config.properties is for this streams application config

    

- build

    `mvn clean compile assembly:single`

    

- run

    `java -Djava.security.auth.login.config=<path of jaas.conf>  -Dlog4j.configuration=file:<path of log4j.properties> -jar kafkastreamsapp-<VERSION>-jar-with-dependencies.jar  <path of config.properties>`