from re import L
from typing import List, Tuple
import pandas as pd
import numpy as np
from random import randint, choices, random
import math


class GA:
    def __init__(self, node_df: pd.DataFrame, service_df: pd.DataFrame) -> None:
        self.node_df = node_df
        self.service_df = service_df
        self.process_df = get_processes_df()
        self.max_cost = node_df["cost"].sum()

    def generate_first_population(
        self,
    ) -> Tuple[List[List], List[List]]:
        allocated_nodes: List[List] = [
            [[]] * len(self.node_df.index) for _ in range(len(self.service_df.index))
        ]
        population = [
            [0] * len(self.node_df.index) for _ in range(len(self.service_df.index))
        ]

        idx = len(self.node_df.index)
        while idx > 0:
            for i, service in self.service_df.iterrows():
                needed = service["node_num"]

                allocated_count = 0
                # Find available nodes
                for j, node in self.node_df.iterrows():
                    if node["status"] == "active":
                        needed -= 1
                        allocated_nodes[i][j] = node
                        population[i][j] = 1
                        idx -= 1
                        allocated_count += 1

                        # Update node capacity, todo
                        self.node_df.at[j, "remain_capacity"] = (
                            node["remain_capacity"] - 1
                        )
                        if needed <= 0:
                            break

                self.service_df.at[i, "node_num"] -= allocated_count

            self.node_df = self.node_df.sort_values("remain_capacity")

        return (allocated_nodes, population)

    def uniform_crossover(self, population: List[List]) -> List:
        offsprings = []

        for i in range(1, len(population), 2):

            # Crossover pair
            p1 = population[i]
            p2 = population[i - 1]

            # Generate crossover mask
            mask = [randint(0, 1) for _ in p1]

            # Create offspring
            o1 = [(p1[i] if mask[i] > 0.5 else p2[i]) for i in mask]
            o2 = [(p2[i] if mask[i] > 0.5 else p1[i]) for i in mask]

            offsprings.append(o1)
            offsprings.append(o2)

        return offsprings

    def mutate(self, population, num_bits, mutation_rate):

        num_pop = len(population)

        # Calculate number of mutations
        num_mutations = math.ceil((num_pop - 1) * num_bits * mutation_rate)

        for _ in range(num_mutations):

            # Pick chromosome
            chromosome_idx = math.ceil(random() * mutation_rate * (num_pop - 1))

            # Pick bit in chromosome
            bit_idx = math.ceil(random() * mutation_rate * num_bits)

            # Flip bit
            population[chromosome_idx][bit_idx] = abs(
                population[chromosome_idx][bit_idx] - 1
            )

        return population

    def tournument_selection(self, population: List[List], desired_num) -> List:
        best_pop: List[List] = []
        best: List = [0 for _ in range(len(population[0]))]
        removed_ind: List[List] = []
        k = 1.0

        while len(best_pop) < desired_num:
            if len(population) == 0:
                population = removed_ind
            rand = randint(0, len(population) - 1)
            ind = population.pop(rand)
            removed_ind.append(ind)

            if self.fitness(ind, rand) > self.fitness(best, rand) * k:
                best_pop.append(ind)
                best = ind
                k = 1
            else:
                k -= 0.1

        return best_pop

    def fitness(self, ind: List, idx) -> float:
        total_cost = 0
        allocated_nodes = 0
        w = 0.3

        for idx, i in enumerate(ind):
            if i == 1:
                total_cost += self.node_df["cost"][idx]
                allocated_nodes += 1

        return w * (1 - total_cost / self.max_cost) + (
            (1 - w) * (allocated_nodes / len(self.node_df.index))
        )

    def pop_fitness(self, pop: List[List]) -> float:
        total_fitness = 0.0
        for idx, ind in enumerate(pop):
            total_fitness += self.fitness(ind, idx)

        return total_fitness


def next_needed_process(process_df: pd.DataFrame) -> pd.DataFrame:
    return process_df.sort(key=lambda x: x.priority, reverse=True)


def next_available_node(individual, avail_nodes):

    # Get order of nodes in individual
    order = [i for i, x in enumerate(individual) if x == 1]

    # Check nodes in that order
    for i in order:
        node = avail_nodes[i]
        if node.active:
            return node


def get_processes_df() -> pd.DataFrame:
    num_processes = 50
    priorities = [1, 2, 3]

    processes = []
    for i in range(num_processes):
        arrival_time = randint(0, 100)
        burst_time = randint(1, 10)
        priority = choices(priorities)[0]

        process = {
            "pid": i,
            "arrivalTime": arrival_time,
            "burstTime": burst_time,
            "priority": priority,
            "turnAroundTime": 0,
            "waitTime": 0,
            "remainingTime": burst_time,
        }

        processes.append(process)

    return pd.DataFrame(processes)
