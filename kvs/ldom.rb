require 'rubygems'
require 'bud'

class DomLattice < Bud::Lattice
  wrapper_name :ldom

  def initialize(i=nil)
    unless i.nil?
      reject_input(i) unless i.kind_of? Hash
      reject_input(i) unless i.keys.all? {|k| k.kind_of? Bud::Lattice}
      reject_input(i) unless i.keys.all? {|v| v.kind_of? Bud::Lattice}
      check_legal_dom(i)
    end
    @v = i
  end

  def merge(i)
    i_val = i.reveal
    return i if @v.nil?
    return self if i_val.nil?

    rv = {}
    preserve_dominants(@v, i_val, rv)
    preserve_dominants(i_val, @v, rv)
    check_legal_dom(rv)
    wrap_unsafe(rv)
  end

  morph :fst do
    compute_reconcile
    @reconcile.first unless @reconcile.nil?
  end

  def snd
    compute_reconcile
    @reconcile.last unless @reconcile.nil?
  end

  private
  def preserve_dominants(target, other, rv)
    target.each_pair do |k1, val|
      # A key/value pair is included in the result UNLESS there is another key
      # in the other merge input that dominates it. Note that there can be at
      # most one such dominating key in either of the inputs.
      next if other.keys.any? {|k2| k2.merge(k1) == k2 && k1 != k2}
      rv[k1] = val
    end
  end

  private
  def compute_reconcile
    return if @v.nil? or @reconcile
    @reconcile = [@v.keys.reduce(:merge), @v.values.reduce(:merge)]
  end

  # Sanity check: all elements in a dom must be concurrent. That is, no element
  # dominates any other element
  private
  def check_legal_dom(h)
    h.each_key do |k1|
      h.each_key do |k2|
        next if k1.equal? k2    # Don't compare a key to itself (NB: not "==")
        merge = k1.merge(k2)
        raise Bud::Error unless merge == k2.merge(k1)
        raise Bud::Error if merge == k1
        raise Bud::Error if merge == k2
      end
    end
  end
end
