import { MeshNode } from './dist/node.js';
import { MeshProtocols } from './dist/protocols.js';

async function main() {
  console.log('Creating two local nodes...');
  
  const node1 = await MeshNode.create({
    agentName: 'alice',
    enableMdns: true,
    listenPorts: { tcp: 0, ws: 0 },
  });
  const node2 = await MeshNode.create({
    agentName: 'bob',
    enableMdns: true,
    listenPorts: { tcp: 0, ws: 0 },
  });

  await node1.start();
  console.log('Node 1 started:', node1.peerId);
  await node2.start();
  console.log('Node 2 started:', node2.peerId);

  const proto1 = new MeshProtocols(node1.libp2p, { agentName: 'alice' });
  const proto2 = new MeshProtocols(node2.libp2p, { agentName: 'bob' });

  // Wire auto-reply on bob
  proto2.onRequest = async (_peerId, req) => {
    return '[auto-response] Received: "' + req.message + '"';
  };

  // Wait for mDNS discovery
  console.log('Waiting for peer discovery...');
  await new Promise(r => setTimeout(r, 8000));
  
  const peers = node1.getPeers().filter(p => p.id !== node1.peerId);
  console.log('Node1 sees peers:', peers.map(p => p.id));

  if (peers.length === 0) {
    console.log('ERROR: No peers discovered');
    await proto1.stop();
    await proto2.stop();
    await node1.stop();
    await node2.stop();
    process.exit(1);
  }

  const targetId = peers[0].id;
  console.log('Sending message to:', targetId);

  // Test 1: simple auto-reply
  try {
    const resp = await proto1.sendMessage(targetId, {
      protocol: '/pi-agent/0.1.0',
      requestId: 'test-1',
      fromAgent: 'alice',
      message: 'Hello Bob!',
      autoReply: true,
    });
    console.log('Test 1 SUCCESS:', JSON.stringify(resp));
  } catch (err) {
    console.log('Test 1 FAILED:', err.message);
  }

  // Test 2: larger payload
  try {
    const bigMsg = 'x'.repeat(10000);
    const resp = await proto1.sendMessage(targetId, {
      protocol: '/pi-agent/0.1.0',
      requestId: 'test-2',
      fromAgent: 'alice',
      message: bigMsg,
      autoReply: true,
    });
    console.log('Test 2 SUCCESS: response length =', resp.message.length);
  } catch (err) {
    console.log('Test 2 FAILED:', err.message);
  }

  await proto1.stop();
  await proto2.stop();
  await node1.stop();
  await node2.stop();
  console.log('Done.');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
