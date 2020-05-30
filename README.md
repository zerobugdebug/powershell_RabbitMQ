# powershell_RabbitMQ
Example of powershell script with RabbitMQ, based on PSRabbitMQ - https://github.com/RamblingCookieMonster/PSRabbitMq

Script is an example of the microservice worker for message processing.
It consumes messages from RabbitMQ queue, process them and post result to RabbitMQ routing key

Routing key is specially generated to support orchestrator-less orchestration, similar to routing slip pattern, but purely based on the RabbitMQ topics.
