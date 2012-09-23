require 'rubygems'
require 'bud'

class DagLattice < Bud::Lattice
  wrapper_name :ldag

  def initialize(i=nil)
    unless i.nil?
      reject_input(i) unless i.kind_of? Hash
      reject_input(i) unless i.keys.all? {|k| k.kind_of? Bud::Lattice}
      reject_input(i) unless i.keys.all? {|v| v.kind_of? Bud::Lattice}
      check_legal_dag(i)
    end
    @v = i
  end

  def merge(i)
    i_val = i.reveal
    return i if @v.nil?
    return self if i_val.nil?

    rv = {}
    @v.each_pair do |k1, val|
      next if i_val.keys.any? {|k2| k2.merge(k1) == k2 && k1 != k2}
      rv[k1] = val
    end

    i_val.each do |k1, val|
      next if @v.keys.any? {|k2| k2.merge(k1) == k2 && k1 != k2}
      rv[k1] = val
    end

    check_legal_dag(rv)
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
  def compute_reconcile
    return unless (@reconcile.nil? && @v != nil)

    merge_key = @v.keys.reduce(:merge)
    merge_val = @v.values.reduce(:merge)
    @reconcile = [merge_key, merge_val]
  end

  # All elements in a dag must be concurrent; i.e., no element dominates any
  # other element
  private
  def check_legal_dag(h)
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
