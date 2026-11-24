// one controller as the default export
const FixtureDefaultAccessController = () => async ({ address }) => ({
  type: 'fixture-default',
  address: address ?? '/fixture-default/acl',
  canAppend: async () => true,
})
FixtureDefaultAccessController.type = 'fixture-default'
export default FixtureDefaultAccessController
