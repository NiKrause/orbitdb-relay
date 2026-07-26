import clsx from 'clsx'
import Link from '@docusaurus/Link'
import useBaseUrl from '@docusaurus/useBaseUrl'
import useDocusaurusContext from '@docusaurus/useDocusaurusContext'
import Layout from '@theme/Layout'
import ThemedImage from '@theme/ThemedImage'

const capabilities = [
  {
    title: 'Pin OrbitDB media',
    body: 'Replicate OrbitDB databases and pin the media CIDs they reference in Helia, so content stays available after writers go offline.',
    to: '/docs/concepts/media-pinning'
  },
  {
    title: 'Bridge browser peers',
    body: 'Act as a circuit-relay v2 and pubsub-discovery node so browser peers behind NAT can find and dial each other.',
    to: '/docs/overview'
  },
  {
    title: 'Serve content over HTTP',
    body: 'Expose /health, /multiaddrs, /pinning/*, /ipfs/* and Prometheus /metrics from a small built-in HTTP server.',
    to: '/docs/reference/http-api'
  },
  {
    title: 'Recover stuck syncs',
    body: 'Work around OrbitDB Sync issue #1255 by re-announcing freshly synced heads to current topic subscribers.',
    to: '/docs/concepts/sync-and-1255-workaround'
  },
  {
    title: 'Terminate TLS in libp2p',
    body: 'AutoTLS provisions a libp2p.direct certificate so the relay advertises secure /tls/ws WebSocket addresses — no nginx required.',
    to: '/docs/getting-started/systemd'
  },
  {
    title: 'Embed as a library',
    body: 'Mount the OrbitDB replication + pinning logic inside your own libp2p node with orbitdbReplicationService().',
    to: '/docs/guides/library'
  }
]

const workflowSteps = [
  'A browser opens an OrbitDB database and subscribes to its pubsub topic',
  'The relay discovers the topic and opens the same database',
  'OrbitDB exchanges log heads; the relay replicates the oplog',
  'The relay extracts media CIDs and pins them in Helia',
  'Later peers sync through the relay and fetch pinned content',
  'POST /pinning/sync recovers a stuck database on demand'
]

export default function Home() {
  const { siteConfig } = useDocusaurusContext()
  const packageVersion = siteConfig.customFields?.packageVersion

  return (
    <Layout
      title="orbitdb-relay"
      description="A pinning and signaling node for local-first OrbitDB + libp2p apps. Replicate databases, pin media, bridge browser peers, and serve content over HTTP."
    >
      <header className="hero hero--shared">
        <div className="container">
          <ThemedImage
            className="hero__mark"
            alt=""
            sources={{
              light: useBaseUrl('/img/orbitdb-relay-mark-light.svg'),
              dark: useBaseUrl('/img/orbitdb-relay-mark-dark.svg')
            }}
          />
          <p className="hero__kicker">Local-first · OrbitDB · libp2p · IPFS pinning</p>
          <h1 className="hero__title">orbitdb-relay</h1>
          <p className="hero__subtitle">
            A long-running pinner and signaling node for OrbitDB and libp2p apps.
          </p>
          <p className="hero__description">
            Local-first apps keep their working data on user devices and replicate peer to peer.
            But browser peers sit behind NAT, go offline, and need somewhere durable to keep shared
            content. <code>orbitdb-relay</code> is that node: it bridges browsers over circuit
            relay, replicates OrbitDB databases, pins the media they reference in Helia, and serves
            it back over a small HTTP API.
          </p>
          <div className="hero__actions">
            <Link className="button button--primary button--lg" to="/docs/getting-started/quickstart">
              Run a relay in 2 minutes
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/overview">
              What is orbitdb-relay?
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/concepts/architecture">
              Read the architecture
            </Link>
          </div>
          <p className="hero__version">Current package version: v{packageVersion}</p>

          <section className="shared-cli" aria-label="orbitdb-relay principles">
            <div className="shared-cli__copy">
              <p className="shared-cli__eyebrow">Data stays with users</p>
              <h2>Replication is OrbitDB's job</h2>
              <p>
                The relay never invents its own replication protocol. OrbitDB owns
                <code> /orbitdb/heads/*</code>, oplog merges, identities, and access control. The
                relay is a well-behaved extra peer: it opens the same databases, keeps them open,
                and lets OrbitDB do the syncing.
              </p>
              <Link className="button button--secondary" to="/docs/concepts/peer-recovery">
                How recovery works
              </Link>
            </div>
            <div className="shared-cli__code">
              <p className="shared-cli__eyebrow">Infrastructure on demand</p>
              <h2>Availability while peers are offline</h2>
              <p>
                Real networks still need signaling, bootstrap, and pinning. The relay bridges
                browser peers over circuit relay v2, pins referenced media in Helia, and keeps
                shared content reachable through <code>/ipfs/&lt;cid&gt;</code> long after the
                original writer has closed their tab.
              </p>
            </div>
          </section>

          <section className="shared-flow" aria-labelledby="workflow-heading">
            <div>
              <p className="shared-cli__eyebrow">From write to durable content</p>
              <h2 id="workflow-heading">What the relay does on every sync</h2>
              <p>
                The relay learns database addresses from live pubsub activity, opens them, and lets
                OrbitDB replicate — then pins the media those records point at.
              </p>
            </div>
            <ol className="shared-flow__steps">
              {workflowSteps.map((step) => (
                <li key={step}>{step}</li>
              ))}
            </ol>
          </section>

          <section className="shared-grid" aria-label="orbitdb-relay capabilities">
            {capabilities.map((card) => (
              <Link key={card.title} className={clsx('shared-card')} to={card.to}>
                <h3>{card.title}</h3>
                <p>{card.body}</p>
              </Link>
            ))}
          </section>
        </div>
      </header>
    </Layout>
  )
}
