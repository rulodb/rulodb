import commonjs from "@rollup/plugin-commonjs";
import {nodeResolve} from "@rollup/plugin-node-resolve";
import typescript from "@rollup/plugin-typescript";
import terser from "@rollup/plugin-terser";

export default {
    input: "src/index.ts",
    output: [
        {
            file: "dist/index.js",
            format: "cjs",
            sourcemap: true,
        },
        {
            file: "dist/index.esm.js",
            format: "esm",
            sourcemap: true,
        },
    ],
    external: [],
    plugins: [
        typescript({
            tsconfig: "./tsconfig.json",
            declaration: true,
            declarationDir: "./dist",
            rootDir: "./src",
        }),
        nodeResolve(),
        commonjs(),
        terser(),
    ],
};
