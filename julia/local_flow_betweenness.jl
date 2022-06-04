using SparseArrays
using Random
using Printf

function local_flow_betweenness(
		nodes::Vector{Int}, edges::Vector{Tuple{Int,Int}};
		locality_index::Real=0.1, timelimit::Real=86400, outdir=nothing)

	ll = get_ll(nodes, edges)

	# `ll` is a List of Lists representation of a network
	# `ll[i]` contains neighbors of node i, for i = 1, 2, ..., n

	return local_flow_betweenness(ll, locality_index=locality_index,
			outdir=outdir, timelimit=timelimit, edges=edges)
end

function local_flow_betweenness(
		ll::Vector{Vector{Int}}; edges=nothing,
		locality_index::Real=0.1, timelimit::Real=86400, outdir=nothing)

    n = length(ll) # number of nodes
    degree = Int[length(ll[i]) for i in 1:n] # degree vector
    vol = sum(degree)
    if isnothing(edges)
	m = Int(vol/2)
	edges = get_edges(ll, n, m)
    end
    seed_mass = Float64(locality_index*vol)
    height = Vector{Float64}(undef, n)
    mass = Vector{Float64}(undef, n)
    score = Dict{Tuple{Int,Int},Float64}(e => 0.0 for e in edges)

    t1 = time_ns()
    runtime = -1.0

    for seed = 1:n

	if degree[seed] == 0
            continue
        end
	if seed % 100 == 0
	    @printf("Processed %d out of %d nodes. Time elapsed: %.3f seconds\n",
				seed, n, runtime)
	end
	l2diffusion!(ll, degree, seed, seed_mass, height, mass)
        nzind = findall(!iszero, mass)
        update_score!(ll, nzind, height, score)

        t2 = time_ns()
        runtime = (t2 - t1)/1e9
        if runtime > timelimit
            break
        end
    end

    @printf("Computation finished. Total running time: %.3f seconds\n", runtime)

    norm = n*seed_mass
    for e in edges
	score[e] /= norm
    end

    if !isnothing(outdir)
	open(outdir, "w") do f
	    for e in edges
		@printf(f, "%.10f\n", score[e])
	    end
	end
    end

    return score
end

function l2diffusion!(ll::Vector{Vector{Int}}, degree::Vector{Int}, seed::Int,
		seed_mass::Float64, height::Vector{Float64}, mass::Vector{Float64};
		itrs::Int=1000, tol::Float64=1.0e-2)

    fill!(height, 0.0)
    fill!(mass, 0.0)
    mass[seed] = seed_mass
    S = Int[seed]
    for k in 1:itrs
        C = [v for v in S if mass[v] > degree[v] + tol]
        if isempty(C)
            break
        end
        for v in shuffle!(C)
            push = (mass[v] - degree[v])/degree[v]
            height[v] += push
            mass[v] = degree[v]
            for u in ll[v]
                if mass[u] == 0
                    push!(S,u)
                end
                mass[u] += push
            end
        end
    end
end

function update_score!(ll, nzind, height, score)
    for i in nzind
        for j in ll[i]
            if j > i
                score[(i,j)] += abs(height[i] - height[j])
            end
        end
    end
end

function get_edges(ll, n::Int, m::Int)
    edges = Vector{Tuple{Int,Int}}(undef, m)
    ct = 0
    for i = 1:n
        for j in ll[i]
            if j > i
                ct += 1
                edges[ct] = (i,j)
            end
        end
    end
    @assert(ct == m)
    return edges
end

function get_ll(nodes::Vector{Int}, edges::Vector{Tuple{Int,Int}})
    if minimum(nodes) != 1
	error("Node index must start from 1.")
    end
    if maximum(nodes) > length(nodes)
	@warn "Input graph has a node index that exceeds total number of nodes."
    end
    ll = [Int[] for _ in nodes]
    for (u,v) in edges
        push!(ll[u], v)
    end
    return ll
end
