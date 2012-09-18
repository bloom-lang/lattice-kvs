require 'rubygems'
require 'bud'

require 'kvs/lpair'

module KvsProtocol
  state do
    channel :kvput, [:reqid, :@addr] => [:key, :val, :client_addr]
    channel :kvput_response, [:reqid] => [:@addr, :replica_addr]
    # If the key does not exist in the KVS, we return a "bottom" value. Since
    # there is not a single bottom value shared across all lattice types (and
    # since we don't have type parameters), we require that the user specify the
    # class to use to construct the bottom value (if needed).
    channel :kvget, [:reqid, :@addr] => [:key, :val_class, :client_addr]
    channel :kvget_response, [:reqid] => [:@addr, :val, :replica_addr]

    # Initiate an async operation to replicate the contents of this replica to
    # the replica at the given address.
    # XXX: currently don't provide a sync API for this functionality
    channel :kvrepl, [:@addr, :target_addr]
  end
end

# Simple KVS replica in which we just merge together all the proposed values for
# a given key. This is reasonable when the intent is to store a monotonically
# increasing lmap of keys.
class KvsReplica
  include Bud
  include KvsProtocol

  state do
    lmap :kv_store
  end

  bloom do
    kv_store <= kvput {|c| {c.key => c.val}}
    kvput_response <~ kvput {|c| [c.reqid, c.client_addr, ip_port]}
    kvget_response <~ kvget {|c| [c.reqid, c.client_addr,
                                  kv_store.at(c.key, c.val_class), ip_port]}
  end
end

module KvsProtocolLogger
  bloom do
    stdio <~ kvput {|c| ["kvput: #{c.inspect} @ #{ip_port}"]}
    stdio <~ kvget {|c| ["kvget: #{c.inspect} @ #{ip_port}"]}
    stdio <~ kvrepl {|c| ["kvrepl: #{c.inspect} @ #{ip_port}"]}
  end
end

class ReplicatedKvsReplica < KvsReplica
  state do
    channel :repl_propagate, [:@addr] => [:kv_store]
  end

  bloom do
    repl_propagate <~ kvrepl {|r| [r.target_addr, kv_store]}
    kv_store <= repl_propagate {|r| r.kv_store}
  end
end

# Simple KVS client that issues put and get operations against a single KVS
# replica. KVS replica address is currently fixed on instanciation.
class KvsClient
  include Bud
  include KvsProtocol

  def initialize(addr, val_class)
    @reqid = 0
    @addr = addr
    @val_class = val_class
    super()
  end

  # XXX: Probably not thread-safe.
  def read(key)
    reqid = make_reqid
    r = sync_callback(:kvget, [[reqid, @addr, key, @val_class, ip_port]], :kvget_response)
    r.each {|t| return t.val if t.reqid == reqid}
    raise
  end

  def write(key, val)
    reqid = make_reqid
    r = sync_callback(:kvput, [[reqid, @addr, key, val, ip_port]], :kvput_response)
    r.each {|t| return if t.reqid == reqid}
    raise
  end

  # XXX: To make it easier to provide a synchronous API, we assume that "to" is
  # local (i.e., we're passed the _instance_ of Bud we want to replicate to, not
  # just its address).
  # XXX: Probably not thread-safe
  def cause_repl(to)
    q = Queue.new
    cb = to.register_callback(:repl_propagate) do |t|
      q.push true
    end
    sync_do { kvrepl <~ [[@addr, to.ip_port]] }

    q.pop
    to.unregister_callback(cb)
  end

  private
  def make_reqid
    @reqid += 1
    "#{ip_port}_#{@reqid}"
  end
end

# More sophisticated KVS client that supports quorum-style operations over a set
# of KVS replicas. Currently, put/get replica sets are fixed on instanciation,
# and we wait for acks from all replicas in the set.
class QuorumKvsClient
  include Bud
  include KvsProtocol

  state do
    table :put_reqs, [:reqid] => [:acks]
    table :get_reqs, [:reqid] => [:acks, :val]
    scratch :w_quorum, [:reqid]
    scratch :r_quorum, [:reqid] => [:val]
  end

  bloom do
    put_reqs <= kvput_response {|r| [r.reqid, Bud::SetLattice.new([r.replica_addr])]}
    w_quorum <= put_reqs {|r|
      r.acks.size.gt_eq(@w_quorum_size).when_true { [r.reqid] }
    }

    get_reqs <= kvget_response {|r| [r.reqid,
                                     Bud::SetLattice.new([r.replica_addr]), r.val]}
    r_quorum <= get_reqs {|r|
      r.acks.size.gt_eq(@r_quorum_size).when_true { [r.reqid, r.val] }
    }
  end

  def initialize(put_list, get_list, val_class)
    @reqid = 0
    @put_addrs = put_list
    @get_addrs = get_list
    @r_quorum_size = get_list.size
    @w_quorum_size = put_list.size
    @val_class = val_class
    super()
  end

  def read(key)
    reqid = make_reqid
    get_reqs = @get_addrs.map {|a| [reqid, a, key, @val_class, ip_port]}
    r = sync_callback(:kvget, get_reqs, :r_quorum)
    r.each {|t| return t.val if t.reqid == reqid}
    raise
  end

  def write(key, val)
    reqid = make_reqid
    put_reqs = @put_addrs.map {|a| [reqid, a, key, val, ip_port]}
    r = sync_callback(:kvput, put_reqs, :w_quorum)
    r.each {|t| return if t.reqid == reqid}
    raise
  end

  private
  def make_reqid
    @reqid += 1
    "#{ip_port}_#{@reqid}"
  end
end
