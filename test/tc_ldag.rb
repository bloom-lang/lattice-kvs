require './test_common'
require 'kvs/ldag'

class RawDagTests < MiniTest::Unit::TestCase
  include LatticeTestSugar

  def test_simple_dominate
    d1 = DagLattice.new(map("k" => max(1)) => set(5))
    d2 = DagLattice.new(map("k" => max(2)) => set(7))
    [d1.merge(d2), d2.merge(d1)].each do |m|
      assert_equal(d2.fst, m.fst)
      assert_equal(d2.snd, m.snd)
    end
  end

  def test_simple_concurrent
    d1 = DagLattice.new(map("k" => max(1)) => set(7))
    d2 = DagLattice.new(map("j" => max(1)) => set(9))
    [d1.merge(d2), d2.merge(d1)].each do |m|
      assert_equal(map("k" => max(1), "j" => max(1)), m.fst)
      assert_equal(set(7, 9), m.snd)
    end
  end
end
