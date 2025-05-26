import path from "node:path";
import {fileURLToPath} from "node:url";

import {FlatCompat} from "@eslint/eslintrc";
import js from "@eslint/js";
import simpleImportSort from "eslint-plugin-simple-import-sort";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all,
});

export default [
    ...compat.extends(
        "eslint:recommended",
        "plugin:@typescript-eslint/recommended",
    ),
    {
        plugins: {
            "simple-import-sort": simpleImportSort,
        },

        languageOptions: {
            ecmaVersion: "latest",
            sourceType: "module",
        },

        rules: {
            semi: ["warn", "always"],
            "simple-import-sort/imports": "error",
            "simple-import-sort/exports": "error",
        },
    },
];
