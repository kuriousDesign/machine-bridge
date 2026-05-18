Generated OPC UA type snapshots from bridge-side inspection scripts live in this folder.

Run `npm run type-generator` from `machine-bridge` to inspect `Machine` and refresh both `types.ts` and `machine-tags.ts` in this folder.

Run `npm run generate-types -- --machine-id=00225` to also inspect `Machine_00225` during the same pass and emit `machine-00225-types.ts` plus `machine-00225-tags.ts`.

You can also use the positional shorthand `npm run generate-types -- 00225`.

The generator source lives at `src/type-generator/generate-machine-types.ts`.

The generator excludes tags whose browse name ends with `fb`, using lowercase comparison for the filter.