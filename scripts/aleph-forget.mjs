// Broadcast a targeted Aleph FORGET for explicitly listed message hashes.
//
// Maintenance tool for retiring replaced production VMs and cleaning leaked
// deployment messages. Deliberately minimal and explicit: it only ever
// forgets the hashes passed in, requires CONFIRM_FORGET=yes, and prints the
// full target list before broadcasting.
//
// Env:
//   ALEPH_PRIVATE_KEY  hex private key of the wallet that sent the messages
//   FORGET_HASHES      comma-separated full item hashes to forget
//   FORGET_CHANNEL     Aleph channel of the original messages
//   FORGET_REASON      human-readable reason recorded in the FORGET
//   ALEPH_API_HOST     default https://api2.aleph.im
//   CONFIRM_FORGET     must be exactly "yes"
import { createHash } from 'node:crypto'
import { privateKeyToAccount } from 'viem/accounts'

const privateKey = process.env.ALEPH_PRIVATE_KEY?.trim()
const hashes = (process.env.FORGET_HASHES || '')
	.split(/[\s,]+/u)
	.map((h) => h.trim())
	.filter(Boolean)
const channel = process.env.FORGET_CHANNEL?.trim() || 'ALEPH-CLOUDSOLUTIONS'
const reason = process.env.FORGET_REASON?.trim() || 'Targeted maintenance forget'
const apiHost = process.env.ALEPH_API_HOST?.trim() || 'https://api2.aleph.im'

if (!privateKey) throw new Error('ALEPH_PRIVATE_KEY is required')
if (hashes.length === 0) throw new Error('FORGET_HASHES is required')
if (hashes.some((h) => !/^[a-f0-9]{64}$/u.test(h)))
	throw new Error('Every FORGET_HASHES entry must be a full 64-char hex item hash')
if (process.env.CONFIRM_FORGET !== 'yes')
	throw new Error('Refusing to broadcast: set CONFIRM_FORGET=yes')

const account = privateKeyToAccount(privateKey.startsWith('0x') ? privateKey : `0x${privateKey}`)
const sender = account.address
const now = Date.now() / 1000

console.log(`FORGET as ${sender} on channel ${channel}:`)
for (const h of hashes) console.log(`  - ${h}`)

const itemContent = JSON.stringify({
	address: sender,
	time: now,
	hashes,
	aggregates: [],
	reason
})
const itemHash = createHash('sha256').update(itemContent, 'utf8').digest('hex')
const signature = await account.signMessage({
	message: ['ETH', sender, 'FORGET', itemHash].join('\n')
})

const message = {
	channel,
	sender,
	chain: 'ETH',
	type: 'FORGET',
	time: now,
	item_type: 'inline',
	item_content: itemContent,
	item_hash: itemHash,
	signature
}

const response = await fetch(new URL('/api/v0/messages', apiHost), {
	method: 'POST',
	headers: { 'content-type': 'application/json' },
	body: JSON.stringify({ sync: true, message })
})
const body = await response.text()
console.log(`broadcast: HTTP ${response.status}`)
console.log(body.slice(0, 500))
if (!response.ok) process.exit(1)
