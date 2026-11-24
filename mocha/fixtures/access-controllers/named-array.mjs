// several controllers in a named `accessControllers` export
const make = (type) => {
  const AccessController = () => async ({ address }) => ({ type, address: address ?? `/${type}/acl`, canAppend: async () => true })
  AccessController.type = type
  return AccessController
}
export const accessControllers = [make('fixture-named-a'), make('fixture-named-b')]
