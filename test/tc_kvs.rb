require './test_common'
require 'kvs/kvs'

class TestMergeMapKvs < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_merge_simple
    r = KvsReplica.new
    r.run_bg
    c = KvsClient.new(r.ip_port, Bud::MaxLattice)
    c.run_bg

    c.write('foo', max(5))
    res = c.read('foo')
    assert_equal(5, res.reveal)

    c.write('foo', max(3))
    res = c.read('foo')
    assert_equal(5, res.reveal)

    c.write('foo', max(7))
    res = c.read('foo')
    assert_equal(7, res.reveal)

    c.stop_bg
    r.stop_bg
  end

  def bump_vc(vc, node_id)
    tmp = vc.merge(map(node_id => max(0)))
    tmp.merge(map(node_id => tmp.at(node_id) + 1))
  end

  def test_vc_simple
    r = KvsReplica.new
    r.run_bg
    c = KvsClient.new(r.ip_port, DomLattice)
    c.run_bg
    c2 = KvsClient.new(r.ip_port, DomLattice)
    c2.run_bg

    # initial version from client c
    new_vc = bump_vc(map, c.ip_port)
    c.write('foo', dom(new_vc => set(5)))
    res = c.read('foo')
    assert_equal(map(c.ip_port => max(1)), res.version)
    assert_equal(set(5), res.value)

    # concurrent version from client c2
    new_vc = bump_vc(map, c2.ip_port)
    c2.write('foo', dom(new_vc => set(3)))
    c2_res = c2.read('foo')
    assert_equal(map(c.ip_port => max(1), c2.ip_port => max(1)), c2_res.version)
    assert_equal(set(3, 5), c2_res.value)

    # c replaces its previous version -- _not_ the merged version that causally
    # reflects c2's update.  (Note the subtetly here: result should be the merge
    # of new c value and c2 value; the old c value must be ignored.)
    new_vc = bump_vc(res.version, c.ip_port)
    c.write('foo', dom(new_vc => set(7)))
    res = c.read('foo')
    assert_equal(map(c.ip_port => max(2), c2.ip_port => max(1)), res.version)
    assert_equal(set(3, 7), res.value)

    # New c write that dominates all previous versions
    new_vc = bump_vc(res.version, c.ip_port)
    c.write('foo', dom(new_vc => set(9)))
    res = c.read('foo')
    assert_equal(map(c.ip_port => max(3), c2.ip_port => max(1)), res.version)
    assert_equal(set(9), res.value)

    [c, c2, r].each {|n| n.stop_bg}
  end

  def test_repl
    nodes = Array.new(3) { ReplicatedKvsReplica.new }
    nodes.each {|n| n.run_bg}
    clients = nodes.map {|n| KvsClient.new(n.ip_port, DomLattice)}
    clients.each {|c| c.run_bg}

    c0, c1, c2 = clients
    new_vc = bump_vc(map, c0.ip_port)
    c0.write('foo', dom(new_vc => set(4)))
    res = c0.read('foo')
    assert_equal({c0.ip_port => 1}, unwrap_map(res.version.reveal))
    assert_equal([4].to_set, res.value.reveal)

    c0.cause_repl(nodes[1])
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1}, unwrap_map(res.version.reveal))
    assert_equal([4].to_set, res.value.reveal)

    new_vc = bump_vc(res.version, c1.ip_port)
    c1.write('foo', dom(new_vc => set(12)))
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1},
                 unwrap_map(res.version.reveal))
    assert_equal([12].to_set, res.value.reveal)

    new_vc = bump_vc(map, c2.ip_port)
    c2.write('foo', dom(new_vc => set(13)))
    res = c2.read('foo')
    assert_equal({c2.ip_port => 1}, unwrap_map(res.version.reveal))
    assert_equal([13].to_set, res.value.reveal)

    c1.cause_repl(nodes[2])
    res = c2.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.version.reveal))
    assert_equal([12,13].to_set, res.value.reveal)

    c2.cause_repl(nodes[1])
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.version.reveal))
    assert_equal([12,13].to_set, res.value.reveal)

    c1.cause_repl(nodes[0])
    res = c0.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.version.reveal))
    assert_equal([12,13].to_set, res.value.reveal)

    (nodes + clients).each {|n| n.stop_bg}
  end
end

class TestQuorumKvs < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_singleton_quorum
    r = KvsReplica.new
    r.run_bg
    q = QuorumKvsClient.new([r.ip_port], [r.ip_port], Bud::MaxLattice)
    q.run_bg

    q.write('bar', max(3))
    res = q.read('bar')
    assert_equal(max(3), res)

    r.stop_bg
    q.stop_bg
  end

  def test_quorum_write_all
    nodes = Array.new(3) { KvsReplica.new }
    nodes.each {|n| n.run_bg }
    addr_list = nodes.map {|n| n.ip_port}
    q = QuorumKvsClient.new(addr_list, addr_list, Bud::MaxLattice)
    q.run_bg

    q.write('bar', max(7))
    nodes.each do |n|
      c_for_n = KvsClient.new(n.ip_port, Bud::MaxLattice)
      c_for_n.run_bg
      res = c_for_n.read('bar')
      assert_equal(max(7), res)
      c_for_n.stop
    end

    (nodes + [q]).each {|n| n.stop}
  end

  def test_quorum_write_one_read_all
    nodes = Array.new(5) { KvsReplica.new }
    nodes.each {|n| n.run_bg }
    addr_list = nodes.map {|n| n.ip_port}
    q = QuorumKvsClient.new([addr_list.last], addr_list, Bud::SetLattice)
    q.run_bg

    q.write('baz', set(2))
    res = q.read('baz')
    assert_equal(set(2), res)

    (nodes + [q]).each {|n| n.stop}
  end
end
