# voltdb-teradata-connector

## Teradata Listener
Teradata Listener captures data from different sources to be ingested into Teradata or Hadoop. Listener 'Sources' provide REST endpoints that can ingest messages at a high rate. One or more 'Targets' can be provided to a Source to act as the destination for the messages. All of this can be setup by using the web interface provided by Listener.

To set up an export connection from VoltDB to a Teradata Listener, first setup the Source using the web interface and use the helpful Ingest code sample for the HTTP headers that need to be set to send messages to the Source. The cURL request looks like below - 

`curl \
  -H "Content-Type: application/json" \
  -H "Authorization: token 87v8069f-1633-4ce1-v548-b4894f9v1874" \
  -X POST -d '{"testing":"123"}' \
  -i \
  https://listener-ingest-services.labs.teradata.com/message`
  
Give the above cURL command a try to make sure that your Listener Source is setup correctly and that your target Teradata or Hadoop system is able to receive the messages correctly.

## VoltDB Export
VoltDB provides the capability to export data from VoltDB streams to another system via pre-built or custom connectors. The voltdb-teradata-connector will allow you to export your data from VoltDB to Teradata Listener over HTTP. 

To configure your instance of VoltDB to export data out to Listener, you'll need to do two things - Configure this export connector on your VoltDB instance and setup your streams to use the connector.

### Export Configuration

1. Check out this code, compile it and build a connector jar file.
2. You can either do this by adding a new Export connector using the Volt Management Console or by editing your deployment.xml to configure this connector as a custom connector by adding a new export element - `<export>
        <configuration enabled="true" target="listener_target" type="custom" exportconnectorclass="org.voltdb.exportclient.TeradataListenerExportClient">
        <property>...</property>
</export>`

3. Add property elements to set the HTTP headers (Content-Type and Authorization) that Listener expects with values from the Ingest code sample above. Also add a property named `endpoint` to set the endpoint URL of your Listener source. 

### Stream Setup

To use the 'listener_target' configured above, you'll need to create a VoltDB stream that targets the 'listener_target' and write to it the data that needs to be exported. 

You can create a stream using - `create stream my_stream export to target listener_target (columns...);` Any data that needs to be written out to the stream should be inserted into the stream by using the insert SQL syntax. e.g., `insert into my_stream values (......);`

More information on understanding export and identifying your export strategy is documented here - https://docs.voltdb.com/UsingVoltDB/ChapExport.php.
