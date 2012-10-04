require './test_common'
require 'kvs/lpair'

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
    assert_equal([1, 2, 3].to_set, first)
    assert_equal([4, 5, 6].to_set, last)

    i.p2 <+ pair(set(4), set(4, 25))
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4].to_set, first)
    assert_equal([4, 5, 6, 25].to_set, last)

    i.p3 <+ pair(set(1, 2, 3, 4, 5), set(10))
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4, 5].to_set, first)
    assert_equal([10].to_set, last)

    i.p3 <+ pair(set(1, 2, 3, 4, 5, 6), set())
    i.tick
    first, last = unwrap_pair(i, :p1)
    assert_equal([1, 2, 3, 4, 5, 6].to_set, first)
    assert_equal(Set.new, last)
  end

  def test_pair_vc
    i = SimplePair.new
    i.p2 <+ pair(map("k" => max(1)), set(20))
    i.p3 <+ pair(map(), set(1, 2, 3))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal({"k" => 1}, first_plain)
    assert_equal([20].to_set, last)

    i.p2 <+ pair(map("l" => max(2)), set(21, 22))
    i.p3 <+ pair(map("j" => max(3)), set(23))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal([["j", 3], ["k", 1], ["l", 2]], first_plain.sort)
    assert_equal([20, 21, 22, 23].to_set, last)

    i.p2 <+ pair(map("k" => max(1), "l" => max(2), "j" => max(4)), set(9, 99))
    i.tick
    first, last = unwrap_pair(i, :p1)
    first_plain = unwrap_map(first)
    assert_equal([["j", 4], ["k", 1], ["l", 2]], first_plain.sort)
    assert_equal([9, 99].to_set, last)
  end

  def test_pair_vc_bug
    a = PairLattice.new([map("k" => max(1)), set(5)])
    b = PairLattice.new([map("k" => max(2)), set(3)])
    c = PairLattice.new([map("j" => max(1)), set(7)])

    a_c = a.merge(c)
    a_b = a.merge(b)
    a_b_c1 = a_c.merge(b)
    a_b_c2 = a_b.merge(c)
    # Bug: these should be the same but they are not
    assert(a_b_c1 == a_b_c2, "lpair is not associative (known bug)")
  end
end
