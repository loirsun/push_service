# push_service
We have some php resident programs, and these programs will die after running for a while.
The program is just a consumer of redis pubsub receiving data and process it.
We rewrite the program in golang and leverage the go routine and channel.

