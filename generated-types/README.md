Generated machine-specific bridge type artifacts live here.

Each machine id gets its own subfolder:

- `generated-types/<machineId>/types.ts`
- `generated-types/<machineId>/tags.ts`

Generate or refresh a machine folder with:

`npm run generate-types -- <machineId>`

When a machine id is provided, the generated type schema combines the shared `Machine` root with `Machine_<id>`. The tag artifact keeps the machine-specific root and exposes `Machine` as an additional root, so tag iteration includes both trees.

Example:

`npm run generate-types -- 00251`

Copy the same folder shape into the UI repo with:

`npm run copy-types-ui -- <machineId>`

Example:

`npm run copy-types-ui -- 00251`