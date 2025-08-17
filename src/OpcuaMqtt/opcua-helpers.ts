// Build Node Strings for OPCUA

import { Device, DeviceTypes } from "@kuriousdesign/machine-sdk";


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



