from random import randint
from preprocess import preprocess
from services import priotize_servieces
from ga import GA, List, Tuple
import matplotlib.pyplot as plt

if __name__ == "__main__":
    node_df = preprocess()
    service_df = priotize_servieces()
    ga = GA(node_df, service_df)
    allocated_nodes, current_pop = ga.generate_first_population()

    mutation_rate = 0.9
    update_pop_count = len(current_pop) / 2
    not_change_for_iter = 0

    last_pop = current_pop
    ADJUST_EVERY = 4
    generation_fitness: List[float] = [ga.pop_fitness(current_pop)]

    generation = 0
    while not_change_for_iter != 100:
        if generation % ADJUST_EVERY == 0:
            mutation_rate *= 0.9  # reduce
            update_pop_count *= 0.9  # increase

        best_pop = ga.tournument_selection(current_pop.copy(), update_pop_count)
        updated_pop = ga.mutate(ga.uniform_crossover(best_pop), 5, mutation_rate)

        last_pop = current_pop
        for _ in range(len(updated_pop)):
            current_pop.pop(randint(0, len(current_pop) - 1))
            current_pop.append(updated_pop.pop())

        if sorted(last_pop, key=lambda x: x.count(1)) == sorted(
            current_pop, key=lambda x: x.count(1)
        ):
            not_change_for_iter += 1
        else:
            not_change_for_iter = 0
        generation += 1
        generation_fitness.append(ga.pop_fitness(current_pop))

        best_ch = []
        best_fitness = 0
        for idx, ch in enumerate(current_pop):
            f = ga.fitness(ch, idx)
            if f > best_fitness:
                best_ch = ch
        print(best_ch)

    print(generation_fitness)
    plt.plot(range(0, len(generation_fitness)), generation_fitness)
    plt.show()
