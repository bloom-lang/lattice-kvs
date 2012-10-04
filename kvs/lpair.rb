require 'rubygems'
require 'bud'

# XXX: Note that this lattice implementation has a bug -- it is not a valid
# lattice because it is not associative. In practical terms, that means that the
# final state of a value might depend not only on the values proposed by
# clients, but also by the way in which replicas synchronize their states. See
# test_pair_vc_bug in tc_lpair for an example of the problem.
class PairLattice < Bud::Lattice
  wrapper_name :lpair

  def initialize(i=nil)
    unless i.nil?
      reject_input(i) unless i.length == 2
      reject_input(i) unless i.all? {|v| v.kind_of? Bud::Lattice}
    end
    @v = i
  end

  def merge(i)
    i_val = i.reveal
    return i if @v.nil?
    return self if i_val.nil?

    # Sanity check: if the first element is equal, the second element should be
    # as well
    raise if @v.first == i_val.first && @v.last != i_val.last

    # The lattice API does not currently include a way to tell if one lattice
    # value is \lt another value. Hence, we instead use the merge method as
    # follows: if a.merge(b) == a, then a \gt_eq b must hold.  Similarly, if
    # a.merge(b) == b, we have a \lt_eq b. If neither is the case, the two
    # values must be incomparable, so we fall back to merging the second field.
    merge_first = @v.first.merge(i_val.first)
    if merge_first == @v.first
      return self
    elsif merge_first == i_val.first
      return i
    else
      return self.class.new([merge_first, @v.last.merge(i_val.last)])
    end
  end

  morph :fst do
    @v.first unless @v.nil?
  end

  def snd
    @v.last unless @v.nil?
  end
end
