import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const configDir = fileURLToPath(new URL('.', import.meta.url))
// Repo-root package.json lives two levels up from docs/docusaurus.
const relayPackage = JSON.parse(
  readFileSync(join(configDir, '..', '..', 'package.json'), 'utf8')
)
const packageVersion = relayPackage.version

const config = {
  title: 'orbitdb-relay',
  tagline: 'A pinning and signaling node for local-first OrbitDB + libp2p apps',
  favicon: 'img/favicon.svg',
  url: 'https://nikrause.github.io',
  baseUrl: '/orbitdb-relay/',
  organizationName: 'NiKrause',
  projectName: 'orbitdb-relay',
  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw'
    }
  },
  themes: ['@docusaurus/theme-mermaid'],
  presets: [
    [
      'classic',
      {
        docs: {
          path: 'docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.mjs',
          editUrl: 'https://github.com/NiKrause/orbitdb-relay/edit/main/docs/docusaurus/',
          showLastUpdateTime: true
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css'
        }
      }
    ]
  ],
  customFields: {
    packageVersion
  },
  headTags: [
    {
      tagName: 'link',
      attributes: { rel: 'icon', href: '/orbitdb-relay/favicon.ico', sizes: '48x48' }
    },
    {
      tagName: 'link',
      attributes: { rel: 'apple-touch-icon', href: '/orbitdb-relay/apple-touch-icon.png' }
    }
  ],
  themeConfig: {
    navbar: {
      logo: {
        alt: 'orbitdb-relay',
        src: 'img/orbitdb-relay-logo-horizontal-light.svg',
        srcDark: 'img/orbitdb-relay-logo-horizontal-dark.svg'
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs'
        },
        {
          href: 'https://github.com/NiKrause/orbitdb-relay',
          position: 'right',
          label: 'GitHub'
        },
        {
          to: '/docs/overview/',
          position: 'right',
          label: `v${packageVersion}`
        }
      ]
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Overview',
              to: '/docs/overview/'
            },
            {
              label: 'Quickstart',
              to: '/docs/getting-started/quickstart'
            },
            {
              label: 'Architecture',
              to: '/docs/concepts/architecture'
            }
          ]
        },
        {
          title: 'Reference',
          items: [
            {
              label: 'HTTP API',
              to: '/docs/reference/http-api'
            },
            {
              label: 'Environment variables',
              to: '/docs/reference/environment-variables'
            },
            {
              label: 'CLI reference',
              to: '/docs/reference/cli'
            }
          ]
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/NiKrause/orbitdb-relay'
            },
            {
              label: 'OrbitDB #1255',
              href: 'https://github.com/orbitdb/orbitdb/issues/1255'
            }
          ]
        }
      ],
      copyright: `Copyright ${new Date().getFullYear()} Le-Space · orbitdb-relay v${packageVersion}`
    },
    colorMode: {
      defaultMode: 'dark',
      disableSwitch: false,
      respectPrefersColorScheme: true
    },
    prism: {
      additionalLanguages: ['bash', 'yaml', 'json', 'toml', 'ini']
    }
  }
}

export default config
