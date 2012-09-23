require './test_common'
require 'kvs/ldag'

class RawDagTests < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_simple_dominate
    d1 = DagLattice.new(map("k" => max(1)) => set(5))
    d2 = DagLattice.new(map("k" => max(2)) => set(7))
    [d1.merge(d2), d2.merge(d1)].each do |m|
      assert_equal(1, m.reveal.size)
      assert_equal(d2.fst, m.fst)
      assert_equal(d2.snd, m.snd)
    end
  end

  def test_simple_concurrent
    d1 = DagLattice.new(map("k" => max(1)) => set(7))
    d2 = DagLattice.new(map("j" => max(1)) => set(9))
    [d1.merge(d2), d2.merge(d1)].each do |m|
      assert_equal(2, m.reveal.size)
      assert_equal(map("k" => max(1), "j" => max(1)), m.fst)
      assert_equal(set(7, 9), m.snd)
    end
  end

  def test_fix_lpair_bug
    a = DagLattice.new(map("k" => max(1)) => set(5))
    b = DagLattice.new(map("k" => max(2)) => set(3))
    c = DagLattice.new(map("j" => max(1)) => set(7))

    a_c = a.merge(c)
    a_b = a.merge(b)
    a_b_c1 = a_c.merge(b)
    a_b_c2 = a_b.merge(c)

    assert(a_b_c1 == a_b_c2)
  end
end
