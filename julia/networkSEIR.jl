using OrdinaryDiffEq
using SparseArrays
using LinearAlgebra

function run_network_seir(A::SparseMatrixCSC{Float64,Int},
		ini_cond::Array{Float64,2}, t_span::Tuple{Float64,Float64};
		R_0::Float64=2.5, sigma::Float64=0.4, gamma::Float64=0.2,
		phi::Float64=1.0, beta::Float64=0.0)
    if beta == 0.0
	beta = gamma*R_0*phi
    end
    N = Vector{Float64}(undef, A.n)
    IN = Vector{Float64}(undef, A.n)
    betaAINS = Vector{Float64}(undef, A.n)
    p = (sigma, gamma, beta, A, N, IN, betaAINS)
    prob = ODEProblem(netSEIR!, ini_cond, t_span, p)
    sol = solve(prob, Tsit5(), saveat=1.0)
    return sol.u
end

function netSEIR!(du,u,p,t)
    sigma, gamma, beta, A, N, IN, betaAINS = p
    S = @view u[:,1]
    E = @view u[:,2]
    I = @view u[:,3]
    R = @view u[:,4]
    dS = @view du[:,1]
    dE = @view du[:,2]
    dI = @view du[:,3]
    dR = @view du[:,4]
    sum!(N, u)
    @. IN = I / N
    mul!(betaAINS, A, IN)
    betaAINS .*= S
    betaAINS .*= beta
    @. dS = -1.0*betaAINS
    @. dE = betaAINS - sigma*E
    @. dI = sigma*E - gamma*I
    @. dR = gamma*I
end
