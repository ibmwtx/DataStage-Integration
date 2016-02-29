# DataStage-Integration
The Code allows to invoke ITX maps from DataStage to invoke complex mapping

##WebSphere TX Accelerator

The WebSphere TX Accelerator provides a full function connector to WebSphere TX (WTX) for DataStage
that executes on top of the Java Integration stage. Throughout this document, this component shall be
named the 'WTX Connector'.

###Highlights of the connector's functionality include:

• Works with WebSphere TX 8.2 onwards

• Supports any number of input links and output links which override cards in the WTX map

• Executes a map either per wave or per input row

• Support reject links when executing a map per row

• Supports delimited or fixed format WTX type trees with configurable syntax

• Supports passing an entire card object of any structural complexity via a single column

• Can map DataStage to WTX types either in binary or string forms.

• Supports importing of WTX type trees to DataStage table definitions

• Supports importing of DataStage table definitions to WTX type trees

