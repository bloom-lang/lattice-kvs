require 'rubygems'
require 'bud'

$:.unshift File.join(File.dirname(__FILE__), "..")

gem 'minitest'  # Use the rubygems version of MT, not builtin (if on 1.9)
require 'minitest/autorun'

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

  def dom(h)
    DomLattice.new(h)
  end

  def unwrap_map(m)
    m.merge(m) {|k,v| v.reveal}
  end
end
