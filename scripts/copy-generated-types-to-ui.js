const fs = require("fs");
const path = require("path");

function parseMachineId(argv) {
  const explicitMachineId = argv
    .map((arg) => {
      const match = arg.match(/^--machine-id=(.+)$/);
      return match ? match[1] : null;
    })
    .find(Boolean);

  if (explicitMachineId) {
    return explicitMachineId;
  }

  const positionalMachineId = argv.find((arg) => !arg.startsWith("--"));

  return positionalMachineId ?? null;
}

function parseUiRoot(argv, defaultUiRoot) {
  const explicitUiRoot = argv
    .map((arg) => {
      const match = arg.match(/^--ui-root=(.+)$/);
      return match ? match[1] : null;
    })
    .find(Boolean);

  return explicitUiRoot ? path.resolve(explicitUiRoot) : defaultUiRoot;
}

function copyFileOrThrow(sourcePath, destinationPath) {
  if (!fs.existsSync(sourcePath)) {
    throw new Error(`Missing generated file: ${sourcePath}`);
  }

  fs.copyFileSync(sourcePath, destinationPath);
}

function main() {
  const argv = process.argv.slice(2);
  const machineId = parseMachineId(argv);

  if (!machineId) {
    throw new Error(
      "Usage: node scripts/copy-generated-types-to-ui.js --machine-id=<machineId> [--ui-root=/path/to/machine-ui-heroui-shadcn]",
    );
  }

  const bridgeRoot = path.resolve(__dirname, "..");
  const defaultUiRoot = path.resolve(bridgeRoot, "../machine-ui-heroui-shadcn");
  const uiRoot = parseUiRoot(argv, defaultUiRoot);
  const sourceDir = path.resolve(bridgeRoot, "generated-types", machineId);
  const destinationDir = path.resolve(uiRoot, "generated-types", machineId);
  const currentDestinationDir = path.resolve(uiRoot, "generated-types", "current");

  const destinationTypesPath = path.resolve(destinationDir, "types.ts");
  const destinationTagsPath = path.resolve(destinationDir, "tags.ts");
  const currentDestinationTypesPath = path.resolve(currentDestinationDir, "types.ts");
  const currentDestinationTagsPath = path.resolve(currentDestinationDir, "tags.ts");

  if (!fs.existsSync(uiRoot)) {
    throw new Error(`UI root does not exist: ${uiRoot}`);
  }

  if (!fs.existsSync(sourceDir)) {
    throw new Error(
      `Missing bridge generated-types folder for machineId=${machineId}: ${sourceDir}`,
    );
  }

  fs.mkdirSync(destinationDir, { recursive: true });
  fs.mkdirSync(currentDestinationDir, { recursive: true });

  copyFileOrThrow(path.resolve(sourceDir, "types.ts"), destinationTypesPath);
  copyFileOrThrow(path.resolve(sourceDir, "tags.ts"), destinationTagsPath);
  copyFileOrThrow(path.resolve(sourceDir, "types.ts"), currentDestinationTypesPath);
  copyFileOrThrow(path.resolve(sourceDir, "tags.ts"), currentDestinationTagsPath);

  console.log(`[copy-generated-types-to-ui] machineId=${machineId}`);
  console.log(
    `[copy-generated-types-to-ui] copied ${path.resolve(sourceDir, "types.ts")} -> ${destinationTypesPath}`,
  );
  console.log(
    `[copy-generated-types-to-ui] copied ${path.resolve(sourceDir, "tags.ts")} -> ${destinationTagsPath}`,
  );
  console.log(
    `[copy-generated-types-to-ui] copied ${path.resolve(sourceDir, "types.ts")} -> ${currentDestinationTypesPath}`,
  );
  console.log(
    `[copy-generated-types-to-ui] copied ${path.resolve(sourceDir, "tags.ts")} -> ${currentDestinationTagsPath}`,
  );
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[copy-generated-types-to-ui] ${message}`);
  process.exit(1);
}