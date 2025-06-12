import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function generateTypes(): Promise<void> {
  try {
    const protoPath = path.resolve(__dirname, '../../../proto');
    const outputDir = path.resolve(__dirname, '../src');

    // Ensure output directory exists
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    console.log('Generating TypeScript types from protobuf using ts-proto...');

    // Generate TypeScript types using ts-proto
    const command = [
      'protoc',
      `--plugin=protoc-gen-ts_proto=${path.resolve(__dirname, '../node_modules/.bin/protoc-gen-ts_proto')}`,
      `--ts_proto_out=${outputDir}`,
      '--ts_proto_opt=esModuleInterop=true',
      '--ts_proto_opt=forceLong=string',
      '--ts_proto_opt=useOptionals=messages',
      '--ts_proto_opt=exportCommonSymbols=false',
      '--ts_proto_opt=outputServices=false',
      '--ts_proto_opt=outputClientImpl=false',
      '--ts_proto_opt=useExactTypes=false',
      '--ts_proto_opt=stringEnums=false',
      '--ts_proto_opt=outputJsonMethods=false',
      `--proto_path=${protoPath}`,
      path.join(protoPath, 'rulo.proto')
    ].join(' ');

    console.log('Running command:', command);

    try {
      execSync(command, {
        stdio: 'inherit',
        cwd: path.resolve(__dirname, '..')
      });
    } catch (error) {
      console.error(error);
    }

    console.log('✅ Generated TypeScript types successfully with ts-proto');
  } catch (error) {
    console.error('❌ Error generating types:', error);
    if (error instanceof Error) {
      console.error('Stack:', error.stack);
    }
    process.exit(1);
  }
}

generateTypes();
