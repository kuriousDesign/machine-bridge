Generated machine-specific bridge type artifacts live here.

Each machine id gets its own subfolder:

- `generated-types/<machineId>/types.ts`
- `generated-types/<machineId>/tags.ts`

Generate or refresh a machine folder with:

`npm run generate-types -- --machine-id=<machineId>`

When `--machine-id` is provided, the generator writes only that machine's artifacts into its subfolder.

Example:

`npm run generate-types -- --machine-id=00251`

Copy the same folder shape into the UI repo with:

`npm run copy-generated-types:ui -- --machine-id=<machineId>`