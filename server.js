const ZooKeeper = require('zookeeper');

const config = {
  connect: '127.0.0.1:2181',
  timeout: 2000,
  debug_level: ZooKeeper.constants.ZOO_LOG_LEVEL_WARN,
  host_order_deterministic: false,
};

const client = new ZooKeeper(config);

const ELECTION_ZNODE = '/election'
const Z_NODE_NAME = `${ELECTION_ZNODE}/guid-n_`;

client.on('connect', async () => {
  /**
   * Create a persistant `/election` znode, that will be used to
   * track child nodes attached to it
   */
  const exists = await client.pathExists(ELECTION_ZNODE);
  if(!exists) {
    await client.create(ELECTION_ZNODE, `${client.client_id}`, ZooKeeper.constants.ZOO_PERSISTENT)
  }

  /**
   * Create a sequential znode for each process. Creating this znode will append a unique, sequential ID to the path
   * This path can be used to identify if a node is the leader or not.
   * 
   * For example, if you create three nodes, the paths could be
   * - /election/guid-n_0000000001
   * - /election/guid-n_0000000002
   * - /election/guid-n_0000000003
   */
  const path = await client.create(Z_NODE_NAME, `${client.client_id}`, ZooKeeper.constants.ZOO_EPHEMERAL_SEQUENTIAL);

  const getFullNodePath = (path) => `${ELECTION_ZNODE}/${path}`

  /**
   * This function is used to check if the current node is the leader.
   * If the path that this node created is the smallest/first node in the sequential list,
   * then assume it is the leader
   */
  const electLeader = async () => {
    const children = await client.get_children(ELECTION_ZNODE);
    const smallestNode = children[0];
    const fullPath = getFullNodePath(smallestNode);

    if(fullPath === path) {
      console.log(`${path} is the leader!`)
      /**
       * Kill the node after a bit of time. This lets us test that the leader
       * reassignment works as expected
       */
      // setTimeout(() => {
      //   process.exit();
      // }, 1500)
    }

    /**
     * Find the index of this zNode. If it is the smallest/leader (or doesn't exist), then return.
     * The leader does not need to listen to other nodes dying
     */
    const myIndex = children.findIndex(child => getFullNodePath(child) === path);
    if(myIndex <= 0) {
      return;
    }

    /**
     * Find the next smallest znode and watch it.
     * If the node we're watching dies, then this node becomes the leader because it is by default the next smallest.
     */
    const nextSmallest = getFullNodePath(children[myIndex - 1]);
    await client.w_get(nextSmallest, () => {
      electLeader();
    })
  }

  
  /**
   * Start the leader assignment by electing a leader
   */
   electLeader();
})

client.init(config);