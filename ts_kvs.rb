require 'rubygems'
gem 'minitest'  # Use the rubygems version of MT, not builtin (if on 1.9)
require 'minitest/autorun'

require './kvs'
require './lpair'

class SimplePair
  include Bud

  state do
    lpair :p1
    lpair :p2
    lpair :p3
  end

  bloom do
    p1 <= p2
    p1 <= p3
  end
end

module LatticeTestSugar
  def max(x)
    Bud::MaxLattice.new(x)
  end

  def set(*x)
    Bud::SetLattice.new(x)
  end

  def map(x={})
    raise unless x.kind_of? Hash
    Bud::MapLattice.new(x)
  end

  def pair(x, y)
    PairLattice.new([x, y])
  end

  def unwrap_map(m)
    m.merge(m) {|k,v| v.reveal}
  end
end

class TestPair < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def unwrap_pair(i, sym)
    val = i.send(sym).current_value.reveal
    [val.first.reveal, val.last.reveal]
  end

  def test_pair_max
    i = SimplePair.new
    i.p2 <+ pair(max(5), max(0))
    i.p3 <+ pair(max(4), max(10))
    i.tick
    assert_equal([5, 0], unwrap_pair(i, :p1))
  end

  def test_pair_set
    i = SimplePair.new
    i.p2 <+ pair(set(1, 2, 3), set(4, 5, 6))
    i.p3 <+ pair(set(1, 2), set(7, 8, 9))
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3], first.sort)
    assert_equal([4, 5, 6], last.sort)

    i.p2 <+ pair(set(4), set(4, 25))
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4], first.sort)
    assert_equal([4, 5, 6, 25], last.sort)

    i.p3 <+ pair(set(1, 2, 3, 4, 5), set(10))
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4, 5], first.sort)
    assert_equal([10], last.sort)

    i.p3 <+ pair(set(1, 2, 3, 4, 5, 6), set())
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4, 5, 6], first.sort)
    assert_equal([], last.sort)
  end

  def test_pair_vc
    i = SimplePair.new
    i.p2 <+ pair(map("k" => max(1)), set(20))
    i.p3 <+ pair(map(), set(1, 2, 3))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal({"k" => 1}, first_plain)
    assert_equal([20], last.sort)

    i.p2 <+ pair(map("l" => max(2)), set(21, 22))
    i.p3 <+ pair(map("j" => max(3)), set(23))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal([["j", 3], ["k", 1], ["l", 2]], first_plain.sort)
    assert_equal([20, 21, 22, 23], last.sort)

    i.p2 <+ pair(map("k" => max(1), "l" => max(2), "j" => max(4)), set(9, 99))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal([["j", 4], ["k", 1], ["l", 2]], first_plain.sort)
    assert_equal([9, 99], last.sort)
  end
end

class TestMergeMapKvs < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_merge_simple
    r = KvsReplica.new
    r.run_bg
    c = KvsClient.new(r.ip_port)
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
    c = KvsClient.new(r.ip_port)
    c.run_bg
    c2 = KvsClient.new(r.ip_port)
    c2.run_bg

    new_vc = bump_vc(map, c.ip_port)
    c.write('foo', pair(new_vc, set(5)))
    res = c.read('foo')
    assert_equal({c.ip_port => 1}, unwrap_map(res.fst.reveal))
    assert_equal([5], res.snd.reveal)

    new_vc = bump_vc(map, c2.ip_port)
    c2.write('foo', pair(new_vc, set(3)))
    c2_res = c2.read('foo')
    assert_equal({c.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(c2_res.fst.reveal))
    assert_equal([3,5], c2_res.snd.reveal.sort)

    new_vc = bump_vc(res.fst, c.ip_port)
    c.write('foo', pair(new_vc, set(7)))
    res = c.read('foo')
    assert_equal({c.ip_port => 2, c2.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([3,5,7], res.snd.reveal.sort)

    new_vc = bump_vc(res.fst, c.ip_port)
    c.write('foo', pair(new_vc, set(9)))
    res = c.read('foo')
    assert_equal({c.ip_port => 3, c2.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([9], res.snd.reveal.sort)

    [c, c2, r].each {|n| n.stop_bg}
  end

  def test_repl
    nodes = Array.new(3) { ReplicatedKvsReplica.new }
    nodes.each {|n| n.run_bg}
    clients = nodes.map {|n| KvsClient.new(n.ip_port)}
    clients.each {|c| c.run_bg}

    c0, c1, c2 = clients
    new_vc = bump_vc(map, c0.ip_port)
    c0.write('foo', pair(new_vc, set(4)))
    res = c0.read('foo')
    assert_equal({c0.ip_port => 1}, unwrap_map(res.fst.reveal))
    assert_equal([4], res.snd.reveal)

    c0.cause_repl(nodes[1])
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1}, unwrap_map(res.fst.reveal))
    assert_equal([4], res.snd.reveal)

    new_vc = bump_vc(res.fst, c1.ip_port)
    c1.write('foo', pair(new_vc, set(12)))
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([12], res.snd.reveal)

    new_vc = bump_vc(map, c2.ip_port)
    c2.write('foo', pair(new_vc, set(13)))
    res = c2.read('foo')
    assert_equal({c2.ip_port => 1}, unwrap_map(res.fst.reveal))
    assert_equal([13], res.snd.reveal)

    c1.cause_repl(nodes[2])
    res = c2.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([12,13], res.snd.reveal.sort)

    c2.cause_repl(nodes[1])
    res = c1.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([12,13], res.snd.reveal.sort)

    c1.cause_repl(nodes[0])
    res = c0.read('foo')
    assert_equal({c0.ip_port => 1, c1.ip_port => 1, c2.ip_port => 1},
                 unwrap_map(res.fst.reveal))
    assert_equal([12,13], res.snd.reveal.sort)

    (nodes + clients).each {|n| n.stop_bg}
  end
end

class TestQuorumKvs < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_singleton_quorum
    r = KvsReplica.new
    r.run_bg
    q = QuorumKvsClient.new([r.ip_port], [r.ip_port])
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
    q = QuorumKvsClient.new(addr_list, addr_list)
    q.run_bg

    q.write('bar', max(7))
    nodes.each do |n|
      c_for_n = KvsClient.new(n.ip_port)
      c_for_n.run_bg
      res = c_for_n.read('bar')
      assert_equal(max(7), res)
      c_for_n.stop
    end

    (nodes + [q]).each {|n| n.stop}
  end

  # XXX: disabled for now
  def ntest_quorum_write_one_read_all
    nodes = Array.new(5) { KvsReplica.new }
    nodes.each {|n| n.run_bg }
    addr_list = nodes.map {|n| n.ip_port}
    q = QuorumKvsClient.new([addr_list.last], addr_list)
    q.run_bg

    q.write('baz', set(2))
    res = q.read('baz')
    assert_equal(set(2), res)

    (nodes + [q]).each {|n| n.stop}
  end
end
