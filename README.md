# EVAM MARKETING COMMUNICATION INTEGRATION TEMPLATE
The custom communication template module is a spring boot project (consume topic: customcomTopic).

Event send is mandatory in CommunicationClient send method.  
You can call the helper method generateSuccessCommunicationResponse and generateFailCommunicationResponse in AbstractCommunicationClient abstract class.

Business logic is developed by inheriting the AbstractCommunicationClient abstract class.

For example, there is the MockCommunicationClient class.

**Default Port:** 9999  
**Default Context Path:** /communication-integration-template

**Health Endpoint:** /communication-integration-template/health

## REQUIREMENTS
```
OpenJDK 17
Lombok IntelliJ plugin 
IntelliJ IDEA (Compiler > Annotation Processors > Enable annotation processing active for lombok)
```
## BUILD
```
./mvnw clean package
```
Output: marketing-communication-integration-template-**version**.zip in target folder
## HOW TO RUN
Update application.yml in config folder (change kafka.bootstrap-address and spring.datasource)
```
java -jar marketing-communication-integration-template-__VERSION__.jar
```
### Wrapper (Recommended)
```
chmod +x bin/*
```
#### Start
```
./bin/marketing-communication-integration-template start
```
#### Stop
```
./bin/marketing-communication-integration-template stop
```
#### Status
```
./bin/marketing-communication-integration-template status
```