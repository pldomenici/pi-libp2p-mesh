// Test that all extension source files can be imported
import './src/types.ts';
console.log('types.ts OK');
import './src/tools.ts';
console.log('tools.ts OK');
import './src/protocols.ts';
console.log('protocols.ts OK');
import './src/node.ts';
console.log('node.ts OK');
import './src/index.ts';
console.log('index.ts OK');
console.log('\n✅ All source files compiled and loaded successfully');
