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
    # b dominates a, c is concurrent with both
    a = DagLattice.new(map("k" => max(1)) => set(5))
    b = DagLattice.new(map("k" => max(2)) => set(3))
    c = DagLattice.new(map("j" => max(1)) => set(7))

    a_c = a.merge(c)
    a_b = a.merge(b)
    a_b_c1 = a_c.merge(b)
    a_b_c2 = a_b.merge(c)

    assert(a_b_c1 == a_b_c2)
  end

  def test_concurrent_perms
    d1 = DagLattice.new(map("a" => max(1)) => set(2))
    d2 = DagLattice.new(map("b" => max(1)) => set(3))
    d3 = DagLattice.new(map("c" => max(1)) => set(4))
    d4 = DagLattice.new(map("d" => max(1)) => set(5))
    all = [d1, d2, d3, d4]

    simple_merge = all.reduce(:merge)
    assert_equal(4, simple_merge.reveal.size)
    assert_equal(map("a" => max(1), "b" => max(1), "c" => max(1), "d" => max(1)),
                 simple_merge.fst)
    assert_equal(set(2,3,4,5), simple_merge.snd)

    check_permutations(all)
  end

  def check_permutations(ops)
    ops.permutation.each do |x|
      ops.permutation.each do |y|
        assert(x.reduce(:merge) == y.reduce(:merge))
      end
    end
  end

  def test_replace_perms
    d1 = DagLattice.new(map("a" => max(1)) => set(7))
    d2 = DagLattice.new(map("a" => max(2)) => set(8))
    d3 = DagLattice.new(map("a" => max(3)) => set(9))
    d4 = DagLattice.new(map("a" => max(4)) => set(0))
    all = [d1, d2, d3, d4]

    simple_merge = all.reduce(:merge)
    assert_equal(1, simple_merge.reveal.size)
    assert_equal(map("a" => max(4)), simple_merge.fst)
    assert_equal(set(0), simple_merge.snd)

    check_permutations(all)
  end

  def test_replace_concurrent_mix_perms
    d1 = DagLattice.new(map("a" => max(1)) => set(2))
    d1_next = DagLattice.new(map("a" => max(2)) => set(7))
    d2 = DagLattice.new(map("b" => max(1)) => set(3))
    d3 = DagLattice.new(map("c" => max(1)) => set(4))
    all = [d1, d1_next, d2, d3]

    simple_merge = all.reduce(:merge)
    assert_equal(3, simple_merge.reveal.size)
    assert_equal(map("a" => max(2), "b" => max(1), "c" => max(1)),
                 simple_merge.fst)
    assert_equal(set(3,4,7), simple_merge.snd)

    check_permutations(all)
  end

  def test_replace_multi_perms
    d1 = DagLattice.new(map("a" => max(1)) => set(3))
    d1_next = DagLattice.new(map("a" => max(2)) => set(2))
    d2 = DagLattice.new(map("b" => max(1)) => set(7))
    d2_next = DagLattice.new(map("b" => max(2)) => set(5))
    d3 = DagLattice.new(map("c" => max(1)) => set(11))
    all = [d1, d1_next, d2, d2_next, d3]

    simple_merge = all.reduce(:merge)
    assert_equal(3, simple_merge.reveal.size)
    assert_equal(map("a" => max(2), "b" => max(2), "c" => max(1)),
                 simple_merge.fst)
    assert_equal(set(2, 5, 11), simple_merge.snd)

    check_permutations(all)
  end
end
