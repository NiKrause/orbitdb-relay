const sidebars = {
  docsSidebar: [
    'overview/index',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/quickstart',
        'getting-started/docker-compose',
        'getting-started/systemd'
      ]
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/architecture',
        'concepts/sync-and-1255-workaround',
        'concepts/media-pinning',
        'concepts/peer-recovery'
      ]
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/access-controllers',
        'guides/identity-providers',
        'guides/library',
        'guides/libp2p-integration'
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/http-api',
        'reference/cli',
        'reference/environment-variables',
        'reference/version-compatibility',
        'reference/ports'
      ]
    }
  ]
}

export default sidebars
