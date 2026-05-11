import {
    ClientSession,
    NodeId,
    coerceNodeId,
    AttributeIds,
    StatusCodes,
    Variant,
    DataType,
    ExtensionObject,
    StatusCode,
    WriteValueOptions,
    DataValue,
} from "node-opcua";

// Build Node Strings for OPCUA

export function buildOpcuaNodePrefix(controllerName: string, nodeId: string): string {
  return `ns=1;s=${nodeId}`;
}

// create a method that takes in a data type and decomposes it to a string array of its members
const decomposeDataType = (dataType: any): string[] => {
  const members: string[] = [];
  for (const key in dataType) {
    if (dataType.hasOwnProperty(key)) {
      members.push(`${key}: ${typeof dataType[key]}`);
    }
  }
  return members;
};

// create list of nodes based on input data type and base node prefix
const createNodeListFromDataType = (dataType: any, baseNodePrefix: string): string[] => {
  // decompose the data type to get its members
  const members = decomposeDataType(dataType);
  // create the node strings for each member
  const nodeList = members.map(member => `${baseNodePrefix}.${member}`);
  return nodeList;
};


// Add this helper method to the class (recursive for nested arrays/objects)
export function toPlainObjects(value: any): any {
  if (Array.isArray(value)) {
    return value.map(item => toPlainObjects(item));
  } else if (value !== null && typeof value === 'object') {
    const plainObj: any = {};
    for (const key in value) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        plainObj[key] = toPlainObjects(value[key]);
      }
    }
    return plainObj;
  }
  return value; // Primitives remain unchanged
}





/**
 * Writes a structured value (ExtensionObject) to an OPC UA node.
 * Safely determines the typeId by reading the DataType attribute (AttributeIds.DataType).
 *
 * @param session - Active OPC UA client session
 * @param nodeId - NodeId of the variable to write (string or NodeId)
 * @param value - Plain JavaScript object matching the structure
 * @returns The status code of the write operation
 */
export async function writeExtensionObjectOld(
    session: ClientSession,
    nodeId: string | NodeId,
    value: Record<string, any>
): Promise<void> {

  


    const targetNodeId = coerceNodeId(nodeId);

    // Step 1: Read the DataType attribute (this gives us the correct typeId for ExtensionObject)
    let dataTypeResult;
    try {
        dataTypeResult = await session.read({
            nodeId: targetNodeId,
            attributeId: AttributeIds.DataType,  // ← this is the key change
        });
    } catch (err) {
        throw new Error(`Failed to read DataType attribute: ${err}`);
    }

    if (dataTypeResult.statusCode !== StatusCodes.Good) {
        throw new Error(
            `Cannot determine structure type - DataType read failed: ${dataTypeResult.statusCode.toString()}`
        );
    }

    const dataTypeNodeId = dataTypeResult.value.value as NodeId;
    if (!dataTypeNodeId) {
        throw new Error("DataType attribute returned null/empty NodeId");
    }

    console.log(`Detected structure DataType NodeId: ${dataTypeNodeId.toString()}`);

    // Optional: You can also check if it's really an ExtensionObject-compatible type
    // (most servers expect the DataType to be the Structure subtype NodeId)

    // Step 2: Create new ExtensionObject using the discovered typeId
    const newExtObj = new ExtensionObject({
        typeId: dataTypeNodeId,    // ← use this instead of runtime value's typeId
        body: value,               // your plain object
    });

    // Step 3: Prepare variant
    const variantToWrite = new Variant({
        dataType: DataType.ExtensionObject,
        value: newExtObj,
    });

    // Step 4: Write
    const writeResult = await session.write({
        nodeId: targetNodeId,
        attributeId: AttributeIds.Value,
        value: {
            value: variantToWrite,
            statusCode: StatusCodes.Good,
        },
    } as WriteValueOptions);

    if (writeResult !== StatusCodes.Good) {
        console.warn(`Write returned: ${writeResult.toString()}`);
    }
}


/**
 * Writes a JSON object (POJO) to an OPC UA node as an ExtensionObject.
 *
 * @param {ClientSession} session The active OPC UA client session.
 * @param {string|NodeId} nodeIdToWrite The NodeId of the target OPC UA variable.
 * @param {object} jsonValue The plain JavaScript object (JSON) corresponding to the structure definition.
 */
export async function writeExtensionObject(session: ClientSession, nodeIdToWrite: string, jsonValue: object): Promise<void> {
    if (!session || !session.write) {
        throw new Error("Invalid OPC UA session provided.");
    }
    const jsonRecord = jsonValue as Record<string, any>;

    console.log(`Attempting to write to NodeId: ${nodeIdToWrite}`);

    // 1. Determine the exact DataType NodeId for the target variable
    const dataTypeValue = await session.read({
        nodeId: nodeIdToWrite,
        attributeId: AttributeIds.DataType
    });

    const dataTypeNodeId = dataTypeValue.value.value;
    console.log(`Target DataType NodeId is: ${dataTypeNodeId.toString()}`);

    // 2. Use the session's internal mechanism to construct the ExtensionObject from the JSON
    let extensionObject;
    try {
        console.log("Attempting to construct ExtensionObject from JSON payload...");
        // This function requires the client to have the target structure definition loaded
        extensionObject = session.constructExtensionObject(dataTypeNodeId, jsonRecord);
        console.log("ExtensionObject constructed successfully.");

    } catch (err) {
        console.error("Failed to construct ExtensionObject. Ensure the server's NodeSet2 file is loaded in the client definition.");
        throw err;
    }

    // --- ADDED VALIDATION HERE ---
    // If construction works but returns a null or undefined, the Variant constructor will fail.
    if (!extensionObject || !(extensionObject instanceof ExtensionObject)) {
        const errorMsg = "Construction resulted in an invalid object. Cannot write to OPC UA server.";
        console.error(errorMsg);
        // Log the problematic JSON value for debugging
        console.error("Problematic JSON input was:", JSON.stringify(jsonValue)); 
        throw new Error(errorMsg);
    }

    // 3. Wrap the ExtensionObject in a Variant and DataValue
    // This part, which previously threw the error, should now succeed because 
    // we guaranteed 'extensionObject' is a valid ExtensionObject instance.
    const dataValue = new DataValue({
        value: new Variant({
            dataType: DataType.ExtensionObject,
            value: extensionObject 
        })
    });

    // 4. Write the DataValue to the server
    const statusCodes = await session.write([
        {
            nodeId: nodeIdToWrite,
            attributeId: AttributeIds.Value,
            value: dataValue
        }
    ]);

    // 5. Handle the result
    if (statusCodes[0].isNotGood()) {
        console.error("Write operation failed: ", statusCodes[0].toString());
        throw new Error(`OPC UA write failed with status: ${statusCodes[0].name}`);
    } else {
        console.log("Write operation successful.");
    }
}

module.exports = { writeExtensionObject };
